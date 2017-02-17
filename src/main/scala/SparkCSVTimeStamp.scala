//How to handle Current_timestamp() in Spark SQL
//
import org.apache.spark.sql.SparkSession

/**
  * Created by vgiridatabricks on 8/25/16.
  */
object SparkCSVTimeStamp {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark File Compression Handling")
      .master("local")
      .getOrCreate()

    val df1 = spark.sql("select current_timestamp() as dateNow")
    val df2 = spark.sql("select cast(current_timestamp() as string) as dateNow")
    val df3 = spark.sql("select from_unixtime(unix_timestamp()) as dataNow")


    //dateFormat has no effect. Does not control the format.
    df1.write.mode("overwrite").format("com.databricks.spark.csv").option("dateFormat", "MM/dd/yyyy HH:mm:ss").save("/tmp/currenttimestamp_no_cast") //1472149120948000
    df2.write.mode("overwrite").format("com.databricks.spark.csv").option("dateFormat", "MM/dd/yyyy HH:mm:ss").save("/tmp/currenttimestamp_cast_string") //2016-08-25 11:18:41.814
    df3.write.mode("overwrite").format("com.databricks.spark.csv").option("dateFormat", "MM/dd/yyyy HH:mm:ss").save("/tmp/unixtime_no_cast") //2016-08-25 11:18:42

  }

}
