package StructStreaming

import org.apache.spark.sql.SparkSession

/**
  * Created by vgiridatabricks on 4/18/18.
  */
object Read220SS {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[2]")
      .appName("Read221Data")
      .getOrCreate()

    val readSS = spark.read.format("parquet").load("/tmp/wcstreaming/220")

    readSS.show(10)
  }

}
