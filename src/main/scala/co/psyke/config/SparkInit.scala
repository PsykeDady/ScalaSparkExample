package co.psyke.config

import org.apache.spark.sql.SparkSession

object SparkInit {
  private var sparkSession: SparkSession = _

  def getSparkSession: SparkSession = {
    synchronized {
      if (sparkSession == null) {
        sparkSession = SparkSession.builder
          .appName("ScalaSparkExample")
          .master("local[2]")  
          .getOrCreate()
      }
      sparkSession
    }
  }
}
