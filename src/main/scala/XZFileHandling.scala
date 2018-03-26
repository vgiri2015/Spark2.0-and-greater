import org.apache.spark.sql.SparkSession

/**
  * Created by vgiridatabricks on 3/25/18.
  */
object XZFileHandling {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Handle XZ File in Spark")
      .master("local")
      .config("skip.header.line.count", "1")
      .getOrCreate()

//    Set this property which handles xz compressed file in spark. This class name is basically adoped from hadoop apis
    spark.sparkContext.hadoopConfiguration.set("io.compression.codecs","io.sensesecure.hadoop.xz.XZCodec")

    val readXZFile = spark.sparkContext.textFile("/Users/vgiridatabricks/Downloads/ec2blobtotxt/xztestfile.txt.xz")

//    Make sure the XZ file is read properly first
    readXZFile.take(100).foreach(println)

    val xzreadDF = spark.read.format("text").option("io.compression.codecs","io.sensesecure.hadoop.xz.XZCodec").load("/Users/vgiridatabricks/Downloads/ec2blobtotxt/xztestfile.txt.xz")

    val header = xzreadDF.first()

//    Remove the first record which is a header
    val finalxzreadDF = xzreadDF.filter(row => row != header)

    finalxzreadDF.createOrReplaceTempView("xztxtfile")

    spark.sql("select split(value,',')[0] as instanceid," +
      "split(value,',')[1] as starttime," +
      "split(value,',')[2] as deletetime," +
      "split(value,',')[3] as hours from xztxtfile").show()

  }

}
