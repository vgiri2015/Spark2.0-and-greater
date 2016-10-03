package structStreaming

import org.apache.spark.sql.SparkSession

/**
  * Created by vgiridatabricks on 10/2/16.
  */
object ReadParquet {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder.master("local")
      .appName("FileToElasticWordCount")
      .getOrCreate()


    //read a text file stream
    val fileRead = spark.read.parquet("/tmp/wcstreaming").show(false)
  }

}
