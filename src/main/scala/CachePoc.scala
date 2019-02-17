import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by vgiridatabricks on 8/8/16.
  */
object CachePoc {

  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL Example")
      .master("local")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()


    val conf = new SparkConf().setAppName("ClusterBy").setMaster("local[2]").set("hive.exec.scratchdir","D:\\hadoop\\")
    val sc = new SparkContext(conf)

    import spark.implicits._

    // Encoders are created for case classes
    val caseClassDS = Seq(Person("Andy", 32)).toDS()

    //    caseClassDS.toDF

    caseClassDS.show()
    caseClassDS.createOrReplaceTempView("df")
    spark.sql("select count(*) from df").show()
  }
}
