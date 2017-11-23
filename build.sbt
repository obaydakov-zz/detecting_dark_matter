name := "detecting_dark_matter"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.2.0" ,
  "org.apache.spark" % "spark-sql_2.11" % "2.2.0" ,
  "org.apache.spark" % "spark-streaming_2.11" % "2.2.0"  ,
  "org.apache.spark" %% "spark-mllib" % "2.2.0",
  // "org.json4s" %% "json4s-native" % "3.2.10",
  // "org.json4s" %% "json4s-native" % "3.2.10",
  //  "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.2.0",
  // "org.apache.kafka" % "kafka-clients" % "1.0.0",
  "com.databricks" % "spark-avro_2.11" % "3.2.0",

  //  "ml.dmlc" %% "xgboost4j-spark" % "0.7"

  "ai.h2o" % "sparkling-water-core_2.11" % "2.2.2",
  "ai.h2o" % "sparkling-water-ml_2.11" % "2.2.2",
  "ai.h2o"  % "sparkling-water-repl_2.11" % "2.2.2"

)

//libraryDependencies += "ai.h2o" % "h2o-scala_2.11" % "3.14.0.7" pomOnly()


resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
resolvers += "Hortonworks Repository" at "http://repo.hortonworks.com/content/repositories/releases/"
resolvers += "Hortonworks Jetty Maven Repository" at "http://repo.hortonworks.com/content/repositories/jetty-hadoop/"
resolvers += Resolver.mavenLocal
resolvers += "central maven" at "https://repo1.maven.org/maven2/"