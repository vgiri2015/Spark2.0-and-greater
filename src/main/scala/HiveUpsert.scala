import FileCompression.DataFrameSample
import org.apache.spark.sql.SparkSession

/**
  * Created by vgiridatabricks on 8/19/16.
  */
object HiveUpsert {

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

    spark.sql("drop table if exists hivetesttable ")
    df.write.saveAsTable("hivetesttable")


    spark.sql("insert into table hivetesttable values ('Inserted Record','Inserted Record','Inserted Record')")
    //    spark.sql("delete from hivetesttable where name='Bart'")
    //    spark.sql("update hivetesttable SET name= 'Updated Record' where name='Homer'")

    spark.sql("select * from hivetesttable").show()
  }
}
