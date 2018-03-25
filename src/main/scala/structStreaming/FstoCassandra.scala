package structStreaming

import java.util.Locale

import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{Dataset, ForeachWriter, Row, SparkSession}

/**
  * Created by vgiridatabricks on 4/12/17.
  */

object FstoCassandra {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder.master("local")
      .config("spark.cassandra.connection.host", "127.0.0.1")
      .appName("FS to Cassandra")
      .getOrCreate()

    //read a text file stream
    val textssc = spark.readStream.text("/Users/vgiridatabricks/Downloads/ssc2.0/")

    import spark.implicits._

    val words = textssc.as[String].flatMap(_.split(" "))

    val wordsDF = words.toDF("word")

    val cassandraWriter = new ForeachWriter[Row] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Row): Unit = {
        val writeToCassandra = value.schema.toDF("value")

        writeToCassandra.toDF().write.mode("append").format("org.apache.spark.sql.cassandra")

//        Seq("a1","b1").toDF().write.mode("append").format("org.apache.spark.sql.cassandra")
          .options(Map("table" -> "wc", "keyspace" -> "spark"))
          .save()
      }
      override def close(errorOrNull: Throwable): Unit = {}
    }
    val query = wordsDF.writeStream.outputMode(OutputMode.Append).format("console").foreach(cassandraWriter).start()
    query.awaitTermination()
  }
}
