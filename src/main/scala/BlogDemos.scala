import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * Created by vgiridatabricks on 10/18/16.
  */
object BlogDemos {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Sample Demo")
      .master("local")
      .getOrCreate()

    val collections = spark.sparkContext.parallelize(1 to 5).collect().foreach(println)


  }
}