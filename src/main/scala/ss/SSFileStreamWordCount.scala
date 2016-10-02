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

    import spark.implicits._
    //Required to find encoder for type stored in a DataSet

    val words = textssc.as[String].flatMap(_.split(" "))


    //Console Sink write stream
    val wc = words.groupBy("value").count()
    val query = wc.writeStream.outputMode("complete").format("console").start()

    query.awaitTermination()
  }

}
