package StructStreaming

import org.apache.spark.sql.SparkSession

/**
  * Created by vgiridatabricks on 10/1/16.
  */
object SocketToConsole01_Demo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local")
      .appName("SocketToConsoleWordCount")
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
    val wordCounts = words.groupBy("value").count() //This is going to give you [value, count] as attributes.

    //Console Sink
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()

  }
}