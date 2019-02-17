import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by vgiridatabricks on 8/19/16.
  */
object WholeStageCodeGenExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL Example")
      .master("local")
      .config("spark.sql.codegen.wholeStage", "true") //default is true so you don't have to setup this.
      .getOrCreate()

    //    val df = spark.range(1L, 200000L, 1L, 1).toDF()
    val startTime = System.nanoTime
    val df = spark.range(1000L * 1000 * 1000).selectExpr("sum(id)").show()
    val endTime = System.nanoTime
    println(s"Total Execution Time " + (endTime - startTime).toDouble / 1000000000 + " seconds")
    //    df.explain()
  }
}

//Without Code Gen
//== Physical Plan ==
//  HashAggregate(keys=[], functions=[sum(id#0L)])
//+- Exchange SinglePartition
//+- HashAggregate(keys=[], functions=[partial_sum(id#0L)])
//+- Filter (id#0L > 100)
//+- Range (0, 1000, splits=1)

//With Whole Stage Code Gen
//== Physical Plan ==
//  *HashAggregate(keys=[], functions=[sum(id#0L)])
//+- Exchange SinglePartition
//+- *HashAggregate(keys=[], functions=[partial_sum(id#0L)])
//+- *Filter (id#0L > 100)
//+- *Range (0, 1000, splits=1)


//The goal is to leverage whole-stage code generation so the engine can achieve the performance of hand-written code,
// yet provide the functionality of a general purpose engine. Rather than relying on operators for processing data at runtime,
// these operators together generate code at runtime and collapse each fragment of the query, where possible, into a single function
// and execute that generated code instead.