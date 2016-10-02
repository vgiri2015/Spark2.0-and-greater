package ss

import org.apache.spark.sql.SparkSession

/**
  * Created by vgiridatabricks on 10/2/16.
  */
object SSFileStreamtoES {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder.master("local")
      .appName("SSFileStreamToES")
      .getOrCreate()

    //read a text file stream
    val textssc = spark.readStream.text("/Users/vgiridatabricks/Downloads/ssc2.0/")


    //Console Sink write stream
    val wordCounts = textssc.groupBy("value").count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("memory")
      .queryName("EsTable")
      .option("checkpointLocation", "/tmp/wordcount/chkpoint") //Must be provided

      .start()
    //
    val df = spark.sql("select * from EsTable")

    import org.elasticsearch.spark.sql._

    df.saveToEs("wordcount/wc")

    query.awaitTermination()
  }

}
