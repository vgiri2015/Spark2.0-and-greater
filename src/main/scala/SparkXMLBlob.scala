import org.apache.spark.sql.SparkSession

/**
  * Created by vgiridatabricks on 3/24/18.
  */
object SparkXMLBlob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark Blob Data in XML")
      .master("local")
      .getOrCreate()

    //Load the incoming XML file as below and spark_xml library to load it into a dataframe

    val xmlfile = "/Users/vgiridatabricks/Downloads/ec2rpt.xml"
    val readxml = spark.read.format("com.databricks.spark.xml").option("rowTag","message").load(xmlfile)

    val decoded = readxml.selectExpr("_source as source","_time as time","_type as type","detail.blob")

    decoded.show() //Displays the raw blob data


    //Apply base64 decoder on every blob data as below
    val decodethisxmlblob = decoded.rdd
        .map(str => str(3).toString)
        .map(str1 => new String(new sun.misc.BASE64Decoder()
        .decodeBuffer(str1)))

    //Store it in a text file temporarily
    decodethisxmlblob.saveAsTextFile("/Users/vgiridatabricks/Downloads/ec2blobtotxt")

    //Parse the text file as required using spark dataframe.

    val readAsDF = spark.sparkContext.textFile("/Users/vgiridatabricks/Downloads/ec2blobtotxt")
    val header = readAsDF.first()
    val finalTextFile = readAsDF.filter(row => row != header)

    import spark.implicits._

    val finalDF = finalTextFile.toDF()
      .selectExpr(
        ("split(value, ',')[0] as instanceId"),
        ("split(value, ',')[1] as startTime"),
        ("split(value, ',')[2] as deleteTime"),
        ("split(value, ',')[3] as hours")
      )

    finalDF.show()

  }

}
