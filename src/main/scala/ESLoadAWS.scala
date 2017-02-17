package rnd

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by vgiridatabricks on 10/7/16.
  */
object ESLoadAWS {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL Example")
      .master("local")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()


    //    val a = spark.sqlContext.getAllConfs.map(r => r._1+ "->"+r._2)
    //
    val ElasticSearchHost = "search-search-databricks-ud77bt7yk53himxjwrf3xlyiqy.us-east-1.es.amazonaws.com"

    val df = spark.range(1000L * 1000 * 1000).selectExpr("sum(id)")




    val esConfig: Map[String, String] = Map("es.nodes" -> "localhost", "es.port" -> "9200", "es.index.auto.create" -> "true")

    import org.elasticsearch.spark.sql._

    df.saveToEs("shows/simpsons", esConfig)


  }

}

