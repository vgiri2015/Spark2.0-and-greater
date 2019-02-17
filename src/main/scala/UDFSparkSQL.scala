import org.apache.spark.sql._

/**
  * Created by vgiridatabricks on 8/17/16.
  */
object UDFSparkSQL {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL UDF")
      .master("local")
      .getOrCreate()

    val TABLE_PARAMETERS = "x_parameters"
    spark.sql(s"drop table if exists $TABLE_PARAMETERS")
    spark.sql(s"create table $TABLE_PARAMETERS(key string, value string)")


    def define_parameter(in_key: String, in_value: String): String = {
      spark.sql(s"insert into $TABLE_PARAMETERS select cast('$in_key' as String), cast('$in_value' as String)")
      return "Good"
    }

    spark.udf.register("define_parameter", define_parameter _)
    //    spark.sql("select define_parameter(1,2)").show()

    define_parameter("5", "6")

  }
}
