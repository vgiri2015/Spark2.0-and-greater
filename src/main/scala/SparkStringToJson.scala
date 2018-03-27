import org.apache.spark.sql.SparkSession

/**
  * Created by vgiridatabricks on 3/26/18.
  */
object SparkStringToJson {

  case class Dataclass(col1: String, col2: String, col3: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark Read of XZ File")
      .master("local")
      .getOrCreate()

    val r1 = new Dataclass("Hollywood", "Movie", "{\"Titanic\":\"Jack\"}")
    val r2 = new Dataclass("Bollywood", "Movie", "{\"Dangal\":\"Aamirkhan\"}")
    val r3 = new Dataclass("Kollywood", "Movie", "{\"Robo\":\"Rajni\", \"Indian\":\"Kamal\"}")

    val tab = Seq(r1, r2, r3)

    import spark.implicits._

    val df1 = tab.toDF()
    df1.createOrReplaceTempView("df1view")

    spark.sql("select * from df1view").show(false)

//    spark.sql("show functions").collect().foreach(println)

    spark.sql("""CREATE TEMPORARY FUNCTION json_map_udf AS 'brickhouse.udf.json.JsonMapUDF'""")
    spark.sql("select col1,col2,key as col3_key,val as col3_val from df1view " +
      "lateral view explode(json_map_udf(col3, 'string,string')) x as key,val").show(false)
  }

}
