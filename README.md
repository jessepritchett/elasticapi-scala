# elasticapi-scala
Toy REST API for Elasticsearch (using Scala, Akka, Spray)

* Listens on http://localhost:5001 by default
* Connects to Elasticsearch at http://localhost:9200 by default

```
usage: [-l <local url>] [-t <target url>] [-n]
```

## Quickstart

* Install [Scala 2.10+] (http://www.scala-lang.org/download/install.html)
* Install [SBT 0.13+] (http://www.scala-sbt.org/release/tutorial/Setup.html)
* In clone directory:

```
sbt "run [options]"
```

* In a browser, or via curl (shown here):
 
```
curl -XGET localhost:5001/cluster
```

* Returns something like:

``` json
{
cluster_name: "elasticsearch",
status: "green",
timed_out: false,
number_of_nodes: 2,
number_of_data_nodes: 2,
active_primary_shards: 10,
active_shards: 20,
relocating_shards: 0,
initializing_shards: 0,
unassigned_shards: 0,
delayed_unassigned_shards: 0,
number_of_pending_tasks: 0,
number_of_in_flight_fetch: 0,
task_max_waiting_in_queue_millis: 0,
active_shards_percent_as_number: 100
}
```

## API Summary

#### Cluster health
see: https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-health.html

Reduced to three GET use-cases:
```
GET /cluster gives cluster-level health
GET /cluster/{index1,index2,...} gives filtered indices-level health
GET /cluster/{index1,index2,...}/shards gives filtered shard-level health
```
* Instead of timing out on non-existent indices, elasticapi will return a NotFound response
* Wait mechanics, and local flag are not exposed.

#### Index management
see: https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html,
     https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-delete-index.html
     
Reduced to create/delete use cases, and moved to /cluster path:
```
POST /cluster/{index1,index2,...} creates one or more indices
DELETE /cluster/{index1,index2,...} deletes one or more indices
```

* Added ability to create/delete multiple indices at once
* **Authentication required** (inspect code for username/password)

#### Node info
see: https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-nodes-info.html

Reduced to one use-case:
```
GET /nodes gives full info for all nodes
```
* **Authentication required** (inspect code for username/password)
        
#### Cluster reroute: allocate
see: https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-reroute.html

Reduced to one use-case:
```
POST /allocate/{index}/{shard}/{node} allocates the given unassigned shard to the given node
```

* **Authentication required** (inspect code for username/password)
        
#### Cluster reroute: move
see: https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-reroute.html

Reduced to one use-case:
```
POST /allocate/{index}/{shard}/{from_node}/{to_node} moves the given shard from one node to another
```

* **Authentication required** (inspect code for username/password)
