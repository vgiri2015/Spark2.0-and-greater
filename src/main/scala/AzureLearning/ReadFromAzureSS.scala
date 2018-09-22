package AzureLearning

import com.microsoft.azure.eventhubs.impl.EventHubClientImpl
import org.apache.spark.sql.SparkSession
import org.apache.spark.eventhubs.{ConnectionStringBuilder, EventHubsConf, EventPosition}
import org.apache.spark.sql.functions.{explode, split}
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.eventhubs._
import org.apache.spark.eventhubs.{ ConnectionStringBuilder, EventHubsConf, EventPosition }
import org.apache.spark.sql.functions.{ explode, split }
import com.microsoft.azure.eventhubs.impl._


/**
  * Created by vgiridatabricks on 8/14/18.
  */

object ReadFromAzureSS {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Azure Read Events")
      .master("local[4]")
      .getOrCreate()

    val connectionString = ConnectionStringBuilder().setNamespaceName("vgiridatabricks").setEventHubName("eventfrompy").setSasKeyName("RootManageSharedAccessKey").setSasKey("GjI+mGLsP457XN31ApEAMKCWDM9Yb/4bnh57/ucWOBc=").build

    val ehConf = EventHubsConf(connectionString).setMaxEventsPerTrigger(5)

    val eventhubs = spark.readStream
      .format("eventhubs")
      .options(ehConf.toMap)
      .load()

    val query = eventhubs
      .writeStream
      .format("parquet")
      .option("path", "/Users/vgiridatabricks/Documents/RnD/Workspace/Spark2.0-and-greater/src/test/pq")
      .option("checkpointLocation", "/Users/vgiridatabricks/Documents/RnD/Workspace/Spark2.0-and-greater/src/test/chkpoint")
      .trigger(ProcessingTime("1 seconds"))
      .start()
  }
}
