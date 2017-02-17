import FileCompression.DataFrameSample
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession


/**
  * Created by vgiridatabricks on 10/28/16.
  */
object ParquetIssue {

  case class SimpsonCharacter(name: String, actor: String, episodeDebut: String)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL Example")
      .master("local")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    //    val df = spark.createDataFrame(
    //      DataFrameSample("Homer", "Dan Castellaneta", "Good Night") ::
    //        DataFrameSample("Marge", "Julie Kavner", "Good Night") ::
    //        DataFrameSample("Bart", "Nancy Cartwright", "Good Night") ::
    //        DataFrameSample("Lisa", "Yeardley Smith", "Good Night") ::
    //        DataFrameSample("Maggie", "Liz Georges and more", "Good Night") ::
    //        DataFrameSample("Sideshow Bob", "Kelsey Grammer", "The Telltale Head") ::
    //        Nil).toDF()
    //
    //    df.write.format("parquet").mode("overwrite").save("/tmp/samples")

    import org.apache.spark.sql.functions._

    val fileChange = spark.read.parquet("/tmp/samples").withColumn("age", lit(30))

    fileChange.write.format("parquet").mode("overwrite").option("inferSchema", "true").save("/tmp/samples1")


  }

}