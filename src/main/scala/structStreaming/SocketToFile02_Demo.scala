package structStreaming

import org.apache.spark.sql.SparkSession

/**
  * Created by vgiridatabricks on 10/1/16.
  */
object SocketToFile02_Demo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local")
      .appName("SocketToFileWordCount")
      .getOrCreate()


    //Required to find encoder for type stored in a DataSet
    import spark.implicits._

    //Socket Stream
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()


    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))


    // Generate running word count
    val wordCounts = words.groupBy("value").count() //This is going to give you value, count as attributes. This is called as Aggregated Streams


    //File Sink
    val query = words.writeStream //Can not do the writestream on wordCounts dataframe because writestream not supported on streaming aggregated dataframe righ now.
        .outputMode("append") //Supports Append Only Mode
        .format("parquet")
        .option("checkpointLocation", "/tmp/wordcount/chkpoint") //Must be provided
        .option("path", "/tmp/wcstreaming") //Path must be specified
        .start()

    query.awaitTermination()

  }
}
