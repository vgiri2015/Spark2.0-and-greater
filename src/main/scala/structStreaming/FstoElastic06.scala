package structStreaming

import org.apache.spark.sql.SparkSession

/**
  * Created by vgiridatabricks on 10/2/16.
  */
object FstoElastic06 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder.master("local")
      .appName("SSFileStreamToES")
      .getOrCreate()


    //read a text file stream
    val textssc = spark.readStream.text("/tmp/ssc2.0/")

    import spark.implicits._
    //Required to find encoder for type stored in a DataSet

    val words = textssc.as[String].flatMap(_.split(" "))

    //Console Sink write stream
    val wordCounts = words.groupBy("value").count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("memory")
      .queryName("EsTable")
      .option("checkpointLocation", "/tmp/wordcount/chkpoint") //Must be provided
      .start()
    //
    val df = spark.sql("select * from EsTable")

    df.select("value", "count").groupBy("value").count()


    //
    //    import org.elasticsearch.spark.sql._
    //
    //    df.saveToEs("wordcount/wc")

    query.awaitTermination()
  }

}
