import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.sql.SparkSession

/**
  * Created by vgiridatabricks on 2/4/17.
  */
object SparkSequenceFile {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark Seq File Handling")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val readSeqasText = spark.sparkContext.
      sequenceFile("/tmp/XR_DF_20170119012734_0.seq",
        classOf[Text], classOf[Text])

    readSeqasText.foreach(println)

    //When Reading this I understood the file content were splitted by , and <xml path.
    // So I performed the following
    //For Ex.
    //abcd.xml,XR40CDT<xml
    //I am splitting it as abcd.xml,XR40CDT,<xml

    val textToString =
      readSeqasText.map {
        case (filename, content) => (filename.toString(), content.toString())
      }.map {
        case (pos1, pos2) => (pos1, pos2.split("<"))
      }.map {
        case (posa, posb) => (posa, posb(0), '<' + posb(1))
      }


    val finalDF = textToString.toDF("xml1", "xml2", "xml3")

    finalDF.show(3, false)


    //Option2
    //    val table = spark.sql("create table inputseqtable (xmlvalue string) " +
    //      "stored as sequencefile " +
    //      "location  '/tmp/new/'")
    //
    //    table.show(3,false)
    //
    //    spark.table("inputseqtable").show(3,false)

  }

}
