import org.apache.spark.sql.SparkSession

/**
  * Created by vgiridatabricks on 8/9/16.
  */
object NestedDataPrint {

  case class nestedRecord(classification: String, score: Double, fv: String)
  case class fullRecord(num1: Integer, num2: Integer, combined: nestedRecord)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL Example")
      .master("local")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import spark.implicits._

    val myrdd = Seq(fullRecord(10,50,nestedRecord("General",.3493,"normal"))).toDS()
    myrdd.show()

  }
}
