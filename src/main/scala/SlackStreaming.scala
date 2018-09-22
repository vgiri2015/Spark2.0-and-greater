import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.jfarcand.wcs.{TextListener, WebSocket}

import scala.util.parsing.json.JSON
import scalaj.http.Http


/**
  * Created by vgiridatabricks on 4/14/18.
  */
object SlackStreaming {
  def main(args: Array[String]) {
//    val spark = SparkSession
//      .builder()
//      .appName("SlackStreaming")
//      .master("local")
//      .getOrCreate()
//
    val conf = new SparkConf().setMaster("local[5]").setAppName("SlackStreaming")
    val ssc = new StreamingContext(conf, Seconds(1))
    val stream = ssc.receiverStream(new SlackReceiver("xoxp-2499669547-40239049846-346929880995-990fe324441a3c762b6e05efd62cfc6b"))
    stream.print()
    stream.saveAsTextFiles("/tmp/slackreceiver")
    ssc.start()
    ssc.awaitTermination()
}


  /**
    * Spark Streaming Example Slack Receiver from Slack
    */
  class SlackReceiver(token: String) extends Receiver[String](StorageLevel.MEMORY_ONLY) with Runnable with Logging {

    private val slackUrl = "https://slack.com/api/rtm.start"

    @transient
    private var thread: Thread = _

    override def onStart(): Unit = {
      thread = new Thread(this)
      thread.start()
    }

    override def onStop(): Unit = {
      thread.interrupt()
    }

    override def run(): Unit = {
      receive()
    }

    private def receive(): Unit = {
      val webSocket = WebSocket().open(webSocketUrl())
      webSocket.listener(new TextListener {
        override def onMessage(message: String) {
          store(message)
        }
      })
    }

    private def webSocketUrl(): String = {
      val response = Http(slackUrl).param("token", token).asString.body
      JSON.parseFull(response).get.asInstanceOf[Map[String, Any]].get("url").get.toString
    }

  }
}
