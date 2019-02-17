import org.apache.spark.sql.SparkSession


/**
  * Created by vgiridatabricks on 2/14/17.
  */
object SparkReadXZFile {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark Read of XZ File")
      .master("local")
      //      .config("io.compression.codecs","io.sensesecure.hadoop.xz.XZCodec")
      .getOrCreate()

    spark.sparkContext.hadoopConfiguration.set("io.compression.codecs", "io.sensesecure.hadoop.xz.XZCodec")


    //Read as RDD
    val readXZFile = spark.sparkContext.textFile("/users/vgiridatabricks/Downloads/xz/Sep2016.txt.xz")
    readXZFile.take(10).foreach(println)

    //Read as DataFrame

    val readXZasDF = spark.read.format("text").option("io.compression.codecs", "io.sensesecure.hadoop.xz.XZCodec").load("/users/vgiridatabricks/Downloads/xz/Sep2016.txt.xz")

    readXZasDF.show(10, false)

    //Store as Hive Table

    spark.sql(s"DROP TABLE IF EXISTS XZFileTable")
    spark.sql(s"CREATE TABLE XZFileTable (allValues STRING) STORED AS TEXTFILE  LOCATION '/mnt/vgiri/xz/'")

    val result = spark.sql(s"select  " +
      s"split(allValues,',')[0] as starttime, " +
      s"split(allValues,',')[1] as endtime, " +
      s"split(allValues,',')[2] as totalhours, " +
      s"split(allValues,',')[3] as clusterid, " +
      s"split(allValues,',')[4] as clustername, " +
      s"split(allValues,',')[5] as nodetype, " +
      s"split(allValues,',')[6] as createdby " +
      s"from XZFileTable")


    result.show(10, false)


  }
}
