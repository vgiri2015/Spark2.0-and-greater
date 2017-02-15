import org.apache.spark.sql.SparkSession

/**
  * Created by vgiridatabricks on 2/13/17.
  */
object SparkBucketBy {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark Bucket By Example")
      .master("local")
      .config("hive.enforce.bucketing", "true")
      .config("spark.sql.autoBroadcastJoinThreshold", "-1") //Set this for Small Table Bucket Joins ONLY
      .getOrCreate()

    import spark.implicits._

    val df = Seq((1, "a"), (1, "b"), (2, "c"), (2, "d")).toDF("x", "y")

    spark.sql("drop table if exists bucket0")

    df.write.bucketBy(10, "x").saveAsTable("bucket0")

    spark.sql("select * from bucket0 a join bucket0 b on a.x=b.x").explain()

  }

}
