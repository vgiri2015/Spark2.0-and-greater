name := "Spark2.0-and-greater"

version := "1.0"

//Older Scala Version
scalaVersion := "2.11.8"

val overrideScalaVersion = "2.11.8"
val sparkVersion = "2.3.1"
val sparkXMLVersion = "0.4.1"
val sparkCsvVersion = "1.4.0"
val sparkElasticVersion = "2.2.0"
val sscKafkaVersion = "1.6.2"
val sparkMongoVersion = "1.0.0"
val sparkCassandraVersion = "1.6.0"

//Override Scala Version to the above 2.11.8 version
ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

resolvers ++= Seq(
  "All Spark Repository -> bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"
)

libraryDependencies ++= Seq(
  "org.apache.spark"      %%  "spark-core"      %   sparkVersion  exclude("jline", "2.12"),
  "org.apache.spark"      %% "spark-sql"        % sparkVersion excludeAll(ExclusionRule(organization = "jline"),ExclusionRule("name","2.12")),
  "org.apache.spark"      %% "spark-hive"       % sparkVersion,
  "org.apache.spark"      %% "spark-yarn"       % sparkVersion,
  "com.databricks"        %% "spark-xml"        % sparkXMLVersion,
  "com.databricks"        %% "spark-csv"        % sparkCsvVersion,
  "org.apache.spark"      %% "spark-graphx"     % sparkVersion,
  "org.apache.spark"      %% "spark-catalyst"   % sparkVersion,
  "org.apache.spark"      %% "spark-streaming"  % sparkVersion,
  "org.elasticsearch"     %% "elasticsearch-spark"        %     sparkElasticVersion,
  "org.apache.spark"      %% "spark-streaming-kafka"     % sscKafkaVersion,
  "org.mongodb.spark"      % "mongo-spark-connector_2.11" %  sparkMongoVersion,
  "com.stratio.datasource" % "spark-mongodb_2.10" % "0.11.1",
  "io.sensesecure" % "hadoop-xz" % "1.4",
  "com.klout" % "brickhouse" % "0.6.0",
  "org.scalaj" %% "scalaj-http" % "2.2.1",
  "org.jfarcand" % "wcs" % "1.5",
  "net.jpountz.lz4" % "lz4" % "1.3.0",
  "com.microsoft.azure" % "azure-eventhubs-spark_2.11" % "2.3.2",
  "com.databricks" % "dbutils-api_2.11" % "0.0.3"
//  "net.snowflake" %% "spark-snowflake" % "2.4.6"

  // Adding this directly as part of Build.sbt throws Guava Version incompatability issues.
  // Please look my Spark Cassandra Guava Shade Project and use that Jar directly.
  //"com.datastax.spark"     % "spark-cassandra-connector_2.11" % sparkCassandraVersion
)