import org.apache.spark.sql.SparkSession

/**
  * Created by vgiridatabricks on 9/25/18.
  */
object SnowFlake {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Snowflake Testing")
      .master("local")
      .getOrCreate()

    val options = Map("sfUrl" -> "ui14160.snowflakecomputing.com",
      "sfUser" -> "databricks",
      "sfPassword" -> "",
      "sfDatabase" -> "DATABRICKS_DEMO",
      "sfSchema" -> "DATABRICKS_DEMO",
      "sfWarehouse" -> "CONNECTOR_INTEGRATION_TEST")



//    Testing SQL based execution
//    val query = "select AGE, WORKCLASS, EDUCATION, RACE FROM DATABRICKS_DEMO.DATABRICKS_DEMO.ADULT"
//
//    val df = spark.read.format("net.snowflake.spark.snowflake")
//      .options(options)
//      .option("query", query)
//      .load()


//    Retrieving a table
//    val df = spark.read.format("net.snowflake.spark.snowflake")
//      .options(options)
//      .option("dbtable","ADULT" )
//      .load()

//    df.show(100)

//    Testing Union Query
//      Works with https://github.com/snowflakedb/spark-snowflake/tree/spark_2.4-snapshot jar I built
    // in /usr/local/Cellar/spark-snowflake/target/scala-2.11/spark-snowflake_2.11-2.4.7-SNAPSHOT.jar
    // and /Users/vgiridatabricks/.ivy2/cache/net.snowflake/snowflake-jdbc/jars/snowflake-jdbc-3.6.12.jar

    val query = "select AGE, WORKCLASS, EDUCATION, RACE FROM DATABRICKS_DEMO.DATABRICKS_DEMO.ADULT"

    val df1 = spark.read.format("net.snowflake.spark.snowflake")
      .options(options)
      .option("query", query)
      .load()

    val df2 = spark.read.format("net.snowflake.spark.snowflake")
      .options(options)
      .option("query", query)
      .load()

    val df3 = df1.union(df2)

    df3.show(100)

  }
}
