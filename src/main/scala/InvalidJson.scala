import org.apache.spark.sql.SparkSession
import scala.util.parsing.json._

/**
  * Created by vgiridatabricks on 2/7/17.
  */
object InvalidJson {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Handle Invalid JSON")
      .master("local")
      .getOrCreate()

    val invalidJson = spark.read.json("/users/vgiridatabricks/Downloads/invalid.json")

    val selectRecord = invalidJson.select("_corrupt_record") //.show(false)

    val jsonOutput = invalidJson.select("_corrupt_record").foreach {
      row => JSON.parseFull(row.toString())
    }.toString.foreach(println)
  }
}
