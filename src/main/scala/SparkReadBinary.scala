import org.apache.spark.sql.SparkSession

/**
  * Created by vgiridatabricks on 2/9/17.
  */
object SparkReadBinary {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark Seq File Handling")
      .master("local")
      .getOrCreate()

    val xmlfile = "/users/vgiridatabricks/Downloads/blob.xml"

    val readxml = spark.read.format("com.databricks.spark.xml").option("rowTag", "message").load(xmlfile)

    val decoded = readxml.selectExpr("_source as source", "_time as time", "_type as type", "detail.blob")

    val decodethisxmlblob = decoded.rdd.map(str => str(3).toString).map(str1 => new String(new sun.misc.BASE64Decoder().decodeBuffer(str1)))

    decodethisxmlblob.saveAsTextFile("/tmp/blobtotxt")

    import spark.implicits._

    val readAsDF = spark.sparkContext.textFile("/tmp/blobtotxt")
    val header = readAsDF.first()
    val finalTextFile = readAsDF.filter(row => row != header)
    val finalDF = finalTextFile.toDF()
      .selectExpr(
        ("split(value, ',')[0] as sequencenum"),
        ("split(value, ',')[1] as date"),
        ("split(value, ',')[2] as time"),
        ("split(value, ',')[3] as subsystem"),
        ("split(value, ',')[4] as majorfunction"),
        ("split(value, ',')[5] as eventCode"),
        ("split(value, ',')[6] as text")
      )


    finalDF.count()

  }
}
