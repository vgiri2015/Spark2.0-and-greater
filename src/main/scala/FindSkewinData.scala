import org.apache.spark.sql.SparkSession

/**
  * Created by vgiridatabricks on 2/3/17.
  */
object FindSkewinData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark Find skew in the data")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val rdd1 = spark.sparkContext.parallelize(List(("Hadoop PIG Hive"), ("Hive PIG Hadoop"), ("Hadoop Hadoop Hadoop")))

    val rdd2 = rdd1.flatMap(x => x.split(" ")).map(x => (x, 1))

    val rdd3 = rdd2.reduceByKey((x, y) => (x + y))

    val rdd4 = rdd3.takeOrdered(3)(Ordering[Int].reverse.on(x => x._2)) //Descending

    val rdd5 = rdd3.takeOrdered(3)(Ordering[Int].on(x => x._2)) //Ascending

    rdd4.foreach(println)

    rdd5.foreach(println)
  }

}
