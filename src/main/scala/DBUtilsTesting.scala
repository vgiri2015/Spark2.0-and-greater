/**
  * Created by vgiridatabricks on 9/18/18.
  */

import com.databricks.dbutils_v1.DBUtilsHolder

object DBUtilsTesting {
  def main(args: Array[String]): Unit = {
    try {
      print("Success")
      DBUtilsHolder.dbutils.notebook.exit("Failed")
    }
    catch {
      case e: Exception => {
      }

    }
  }
}
