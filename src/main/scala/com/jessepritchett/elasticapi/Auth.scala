package com.jessepritchett.elasticapi

import com.typesafe.config.ConfigFactory
import spray.routing.authentication.{BasicAuth, UserPass}
import spray.routing.directives.AuthMagnet

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by Jesse on 7/18/2016.
  */
object Auth {

  val authdb = Map(
    "admin" -> "admin",
    "joel" -> "coen",
    "ethan" -> "coen",
    "hudsucker" -> "proxy")

  def basicUserAthenticator(implicit ec: ExecutionContext): AuthMagnet[String] = {
    // TODO real authentication
    def check_auth(userPass: Option[UserPass]): Option[String] = {
      for {
        up <- userPass
        if authdb.get(up.user).contains(up.pass)
      } yield up.user
    }

    def authenticator(userPass: Option[UserPass]): Future[Option[String]] = Future { check_auth(userPass) }

    BasicAuth(authenticator _, realm = "private site")
  }

}
