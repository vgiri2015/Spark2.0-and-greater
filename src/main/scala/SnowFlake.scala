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
      "sfPassword" -> "gV6UU*0iS44S",
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
