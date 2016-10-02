package ss

import org.apache.spark.sql.SparkSession
import com.databricks.spark.csv
import org.apache.spark.sql.types.StructType

/**
  * Created by vgiridatabricks on 10/1/16.
  */
object SSFileStreamWordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder.master("local")
      .appName("sscFileStream")
      .getOrCreate()

    //read a text file stream
    val textssc = spark.readStream.text("/Users/vgiridatabricks/Downloads/ssc2.0/")


    //Console Sink write stream
    val wc = textssc.groupBy("word").count()
    val query = wc.writeStream.outputMode("complete").format("console").start()

    query.awaitTermination()
  }

}
