import org.apache.spark.sql.SparkSession
import org.apache.hadoop.io.{BytesWritable, Text}
import com.cotdp.hadoop.ZipFileInputFormat

/**
  * Created by vgiridatabricks on 2/17/17.
  */
object SparkReadZipFile {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Handle Invalid JSON")
      .master("local")
      .getOrCreate()


    val zipFileRDD = spark.sparkContext.newAPIHadoopFile("/users/vgiridatabricks/Downloads/Nov2016.zip", classOf[ZipFileInputFormat],
      classOf[Text],
      classOf[BytesWritable],
      spark.sparkContext.hadoopConfiguration)
      .map { case (a, b) => new String(b.getBytes(), "UTF-8") }

    zipFileRDD.take(5).foreach(println)
  }

}

//Good show