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
