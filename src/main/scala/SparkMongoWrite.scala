import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql.SparkSession

/**
  * Created by vgiridatabricks on 8/22/16.
  */
object SparkMongoWrite {

  case class DataFrameSample(name: String, actor: String, episodeDebut: String)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark File Compression Handling")
      .master("local")
      .getOrCreate()

    val df = spark.createDataFrame(
      DataFrameSample("Homer", "Dan Castellaneta", "Good Night") ::
        DataFrameSample("Marge", "Julie Kavner", "Good Night") ::
        DataFrameSample("Bart", "Nancy Cartwright", "Good Night") ::
        DataFrameSample("Lisa", "Yeardley Smith", "Good Night") ::
        DataFrameSample("Maggie", "Liz Georges and more", "Good Night") ::
        DataFrameSample("Sideshow Bob", "Kelsey Grammer", "The Telltale Head") ::
        Nil).toDF().cache()





    val mongoURI = "mongodb://localhost:27017"
    def makeMongoURI(uri: String, database: String, collection: String) = (s"${uri}/${database}.${collection}")
    val testMongoConf = makeMongoURI(mongoURI, "test", "sparkmongoconnect")
    val writeConfigPeople: WriteConfig = WriteConfig(scala.collection.immutable.Map("uri" -> testMongoConf))
    //    df.write.mongo(writeConfigPeople)


  }
}
