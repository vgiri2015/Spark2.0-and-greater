package ss

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

/**
  * Created by vgiridatabricks on 10/1/16.
  */
object SSSocketWordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder.master("local")
      .appName("StructuredNetworkWordCount")
      .getOrCreate()


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
    val wordCounts = words.groupBy("value").count()

    //Console Sink
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    //File Sink
    //    val fsds = words.toDF("value") //Can not do the writestream on wordCounts dataframe because writestream not supported on streaming aggregated dataframe righ now.
    //    val query = fsds.writeStream
    //      .outputMode("append") //Supports Append Only Mode
    //      .format("parquet")
    ////      .partitionBy("value")
    //      .option("checkpointLocation", "/tmp/wordcount/chkpoint") //Must be provided
    //      .option("path","/tmp/wcstreaming") //Path must be specified
    //      .start()

    //Memory Sink
    //    val query = wordCounts.writeStream
    //      .outputMode("complete")
    //      .format("memory")
    //      .queryName("estable")
    //      .start()
    //    //
    //    val df = spark.sql("select * from estable")

    //    print(wordCounts.printSchema())
    //
    //    print(wordCounts.isStreaming)
    //
    //    print(query.name)
    //
    //    print(query.explain())
    //
    query.awaitTermination()

  }
}
