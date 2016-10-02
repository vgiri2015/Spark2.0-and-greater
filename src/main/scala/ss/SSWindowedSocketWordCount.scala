package ss

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by vgiridatabricks on 10/2/16.
  */
object SSWindowedSocketWordCount {
  def main(args: Array[String]): Unit = {

    val windowDuration = s"10 seconds"
    val slideDuration = s"5 seconds"

    val spark = SparkSession
      .builder
      .master("local")
      .appName("StructuredNetworkWordCountWindowed")
      .getOrCreate()

    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to host:port
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .option("includeTimestamp", true)
      .load()

    //File Stream
    //    val textssc = spark.readStream.text("/Users/vgiridatabricks/Downloads/ssc2.0/")
    //    val wc = textssc.groupBy("word").count()


    // Split the lines into words, retaining timestamps
    val words = lines.as[(String, Timestamp)].flatMap(line =>
      line._1.split(" ").map(word => (word, line._2))
    ).toDF("word", "timestamp")

    //    words.groupBy($"timestamp",window(windowDuration,slideDuration)).count()

    // Group the data by window and word and compute the count of each group
    val windowedCounts = words.groupBy(
      window($"timestamp", windowDuration, slideDuration), $"word"
    ).count().orderBy("window")

    // Start running the query that prints the windowed word counts to the console
    val query = windowedCounts.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .start()

    query.awaitTermination()

  }


}
