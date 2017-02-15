//import org.apache.spark.sql.SparkSession
//
///**
//  * Created by vgiridatabricks on 2/7/17.
//  */
//object SparkSessionError {
//
//  def main(args: Array[String]) : Unit = {
//    val spark = SparkSession
//      .builder()
//      .appName("Spark Multi Threading Example")
//      .master("local")
//      .getOrCreate()
//
//    spark.range(100).map {
//      p => val withinExecutorGetOrCreate = SparkSession.builder.config(spark.sparkContext.getConf)
//        .getOrCreate()
//    }.count
//
//  }
//
//}
