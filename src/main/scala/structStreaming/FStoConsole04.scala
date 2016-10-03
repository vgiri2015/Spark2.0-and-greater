package structStreaming

import org.apache.spark.sql.SparkSession


/**
  * Created by vgiridatabricks on 10/1/16.
  */
object FStoConsole04 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder.master("local")
      .appName("FileToConsoleWordCount")
      .getOrCreate()

    //read a text file stream
    val textssc = spark.readStream.text("/Users/vgiridatabricks/Downloads/ssc2.0/")

    import spark.implicits._
    //Required to find encoder for type stored in a DataSet

    val words = textssc.as[String].flatMap(_.split(" "))

    val wordCounts = words.groupBy("value").count()

    //Console Sink write strea
    val query = wordCounts.writeStream
      .outputMode("complete")
      .option("checkpointLocation", "/tmp/filewordcount/chkpoint")
      .format("console")
      .start()

    query.awaitTermination()
  }

}
