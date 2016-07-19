package com.jessepritchett.elasticapi

import scala.util.{Failure, Success}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import akka.actor.ActorSystem
import akka.io.Tcp.ErrorClosed
import akka.util.Timeout
import spray.can.Http.ConnectionException
import spray.json._
import spray.http._
import spray.http.Uri._
import spray.httpx.SprayJsonSupport._
import spray.client.pipelining._
import spray.routing.authentication.{BasicAuth, UserPass}
import spray.routing.{ExceptionHandler, SimpleRoutingApp}
import spray.util.LoggingContext

/**
  * Created by Jesse on 7/18/2016.
  */
object WebRoot extends App with SimpleRoutingApp {

  // define case data models
  case class Allocate(index: String, shard: String, node: String)
  case class Move(index: String, shard: String, form_node: String, to_node: String)

  // bring marshallers into scope
  object RerouteProtocol extends DefaultJsonProtocol {
    implicit val allocateFormat = jsonFormat3(Allocate)
    implicit val moveFormat = jsonFormat4(Move)
  }
  import RerouteProtocol._

  // declare global ActorSystem and pull system dispatcher into scope
  implicit val system = ActorSystem()
  import system.dispatcher

  // define send/receive pipeline for HTTP Client activities
  // (see: http://spray.io/documentation/1.2.2/spray-client)
  val pipeline: HttpRequest => Future[HttpResponse] = sendReceive

  // parse command line, etc. (will exit if command line can't be parsed)
  var opts = init

  // options
  lazy val local = Uri(opts.getOrElse('local, "http://localhost:5001").asInstanceOf[String])
  lazy val target = Uri(opts.getOrElse('target, "http://localhost:9200").asInstanceOf[String])
  lazy val cert = opts.getOrElse('no_cert, true).asInstanceOf[Boolean]


  // start the server
  startServer(interface = local.authority.host.address, port = local.effectivePort) {
    /** very basic error handling */
    implicit def exceptionHandler(implicit log: LoggingContext) = ExceptionHandler {
      case e: Exception =>
        requestUri { uri =>
          complete(StatusCodes.InternalServerError, s"Couldn't connect to Elasticsearch: ${e.getMessage}")
        }
    }

    // Spray-routing paths
    // (see: http://spray.io/documentation/1.2.3/spray-routing)

    sealRoute {
      path("") {
        get {
          complete(s"Hello from Jesse P.")
        }
      } ~
      pathPrefix("cluster") {
        /*
        Cluster health
        see: https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-health.html

        Reduced to three GET use-cases:
          GET /cluster gives cluster-level health
          GET /cluster/{index1,index2,...} gives filtered indices-level health
          GET /cluster/{index1,index2,...}/shards gives filtered shard-level health

        Wait mechanics, and local flag are not exposed.

        Index management
        see: https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html,
             https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-delete-index.html

        Added ability to create/delete multiple indices at once
          POST /cluster/{index1,index2,...} creates one or more indices
          DELETE /cluster/{index1,index2,...} deletes one or more indices
        */

        pathEnd {
          get { // GET /cluster
            val url = target withPath Path("/_cluster/health")
            complete(pipeline(Get(url)))
          }
        } ~
        pathPrefix(Segment) { indices =>
          pathEnd {
            get { // GET /cluster/{index1,index2,...}
              val url = target withPath Path("/_cluster/health/" + indices) withQuery ("level" -> "indices")
              checkIndices(indices, url)
            } ~
            post { // POST /cluster/{index1,index2,...} (multi-index create)
              authenticate(Auth.basicUserAthenticator) { user =>
                complete {
                  // compose a future of List[HttpResponse] for each POST and map each to List[String] via toString
                  Future.traverse(indices.split(",").toList) { index =>
                    val url = target withPath Path("/" + index)
                    pipeline(Post(url)).map(_.toString)
                  }
                }
              }
            } ~
            delete { // DELETE /cluster/{index1,index2,...} (multi-index delete)
              authenticate(Auth.basicUserAthenticator) { user =>
                complete {
                  // compose a future of List[HttpResponse] for each DELETE and map each to List[String] via toString
                  Future.traverse(indices.split(",").toList) { index =>
                    val url = target withPath Path("/" + index)
                    pipeline(Delete(url)).map(_.toString)
                  }
                }
              }
            }
          } ~
          path("shards") {
            get { // GET /cluster/{index1,index2,...}/shards
              val url = target withPath Path("/_cluster/health/" + indices) withQuery ("level" -> "shards")
              checkIndices(indices, url)
            }
          }
        }
      } ~
      authenticate(Auth.basicUserAthenticator) { user =>
        path("nodes") {
          /*
          Node info
          see: https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-nodes-info.html

          Reduced to one use-case:
            GET /nodes gives full info for all nodes
          */

          get {
            val url = target withPath Path("/_nodes")
            complete(pipeline(Get(url)))
          }
        } ~
        path("allocate" / Segment / Segment / Segment) { (index, shard, node) =>
          /*
          Cluster reroute: allocate
          see: https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-reroute.html

          Reduced to one use-case:
            POST /allocate/{index}/{shard}/{node} allocates the given unassigned shard to the given node
          */

          post { //
            val url = target withPath Path("/_cluster/reroute")
            complete(pipeline(Post(url, Allocate(index, shard, node))))
          }
        } ~
        path("move" / Segment / Segment / Segment / Segment) { (index, shard, from_node, to_node) =>
          /*
          Cluster reroute: move
          see: https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-reroute.html

          Reduced to one use-case:
            POST /allocate/{index}/{shard}/{from_node}/{to_node} moves the given shard from one node to another
          */

          post {
            val url = target withPath Path("/_cluster/reroute")
            complete(pipeline(Post(url, Move(index, shard, from_node, to_node))))
          }
        }
      }
    }
  }

  /** parse arguments, etc */
  def init = {
    val usage = "usage: [-l <local url>] [-t <target url>] [-n]"

    // one way to parse args in Scala...
    def parseOption(map: Map[Symbol, Any], list: List[String]): Map[Symbol, Any] = {
      list match {
        case Nil => map
        case "-l" :: value :: tail if !value.startsWith("-") => parseOption(map ++ Map('local -> value), tail)
        case "-t" :: value :: tail if !value.startsWith("-") => parseOption(map ++ Map('target -> value), tail)
        case "-n" :: tail => parseOption(map ++ Map('no_cert -> true), tail)
        case _ =>
          println(usage)
          System.exit(1)
          null
      }
    }

    parseOption(Map(),args.toList)
  }

  /** check if indices exist, instead of timing out */
  def checkIndices(indices: String, url: Uri) = {
    // compose a future of List[(String,StatusCode)], calling HEAD requests for each index
    val fList = Future.traverse(indices.split(",").toList) { index =>
      pipeline(Head(target withPath Path("/"+index))).map(index -> _.status)
    }

    // use onSuccess, since all HEAD requests will succeed
    onSuccess(fList) { list =>
      list filter { res =>
        res._2 != StatusCodes.OK // (String,StatusCode)
      } match {
        case Nil => complete(pipeline(Get(url)))
        case rest => complete(StatusCodes.NotFound, s"Elasticsearch indices not found: ${rest map (_._1) mkString ", "}")
      }
    }
  }

}
