import org.apache.hadoop.io.Text
import org.apache.spark.sql.SparkSession

/**
  * Created by vgiridatabricks on 2/5/17.
  */
object CreateSeqFile {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Seq File Handling")
      .master("local")
      .getOrCreate()

    //    val txtfile = spark.sparkContext.textFile("/users/vgiridatabricks/Desktop/college.txt")

    //Creating PairRDD from Textfile read
    //    val createKeyBy = txtfile.map(x => (x.split(",")(0), (x.split(",")(1))))
    //                                                  .saveAsSequenceFile("/tmp/txt-seq")

    val seqFileRead = spark.sparkContext.sequenceFile("/tmp/txt-seq/*", classOf[Text], classOf[Text])


    val testit = seqFileRead.take(5).foreach(println)


  }


}
