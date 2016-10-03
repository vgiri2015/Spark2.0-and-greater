package structStreaming

import org.apache.spark.sql.SparkSession

/**
  * Created by vgiridatabricks on 10/2/16.
  */
object FstoElastic06 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder.master("local")
      .appName("FileToElasticWordCount")
      .getOrCreate()


    //read a text file stream
    val textssc = spark.readStream.text("/Users/vgiridatabricks/Downloads/ssc2.0/")

    import spark.implicits._
    //Required to find encoder for type stored in a DataSet

    val words = textssc.as[String].flatMap(_.split(" "))

    //Console Sink write stream
    val wordCounts = words.groupBy("value").count()


    //    val wordsdf = textssc.as[String].flatMap(_.split(" ")).toDF("value")
    //    val query = wordsdf.writeStream
    //      .outputMode("append") //parquet doesn't support complete output mode and at the same time append mode is not supported on wordCounts dataset since it's doing streaming aggregates.
    //      .format("parquet")
    //      .option("path", "/tmp/wcstreaming/parquet1") //Path must be specified
    //      .option("checkpointLocation", "/tmp/wordcount/parquet1") //Must be provided
    //      .start()

    //    val df = spark.read.parquet("/tmp/wcstreaming/parquet")
    //
    //    import org.elasticsearch.spark.sql._
    //
    //    df.saveToEs("wordcount/wc")


    //Trying this using Memory Sink
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("memory")
      .queryName("elastic")
      .start()

    val df = spark.sql("select * from elastic")
    val df1 = df.groupBy("value").count()

    import org.elasticsearch.spark.sql._
    df1.saveToEs("wordcount/wc")

    query.awaitTermination()
  }

}
