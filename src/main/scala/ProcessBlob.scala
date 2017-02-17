import org.apache.spark.sql.SparkSession


/**
  * Created by vgiridatabricks on 2/12/17.
  */
object ProcessBlob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark Process BLOB")
      .master("local")
      .getOrCreate()


    val dataSeq = Seq(("SGVsbG8gV29ybGQ=", "IronMan", "JungleBook"), ("SGVsbG8gV29ybGQ=", "10", "20"), ("SGVsbG8gV29ybGQ=", "30", "40"), ("SGVsbG8gV29ybGQ=", "30", "20"))
    //    spark.sparkContext.parallelize(dataSeq)
    val df = spark.createDataFrame(data = dataSeq)

    import spark.implicits._

    val decodethisxmlblob = df.rdd.map(str => str(0).toString).
      map(str => new String(new sun.misc.BASE64Decoder().decodeBuffer(str))).toDF("decoded")

    //    print(decodethisxmlblob.toDebugString)
  }

}
