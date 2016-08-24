import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


object WordCountFileStreaming {
  def main(args: Array[String]) {

    //StreamingExamples.setStreamingLogLevels()
    val sparkConf = new SparkConf().setAppName("File Streaming Word Count").setMaster("local")

    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    
    //Always listen for Latest directory and make sure it gets the files or records for every batches
    val lines = ssc.textFileStream("/tmp/ssclisten/subfolder1/subfolder2") //Make Sure the File Name doesn't have '.' or ':' Hadoop path API does not support.
    lines.foreachRDD({ rdd =>
      val myFilesList = rdd.toDebugString
      myFilesList.foreach(println)
    })
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.count()
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
