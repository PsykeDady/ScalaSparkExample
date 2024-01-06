package co.psyke.utils.files

import java.nio.file.{Files,Paths}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.Encoders
import co.psyke.models.GameDetail


object PersistUtility {
  val archivePath = "./archive/games" 
  val gamesArchiveFile = s"${archivePath}/games.parquet" 
  val gamesArchiveResult = s"${archivePath}/result.csv" 

  def archiveExists () : Boolean = {
    val path = Paths.get(archivePath)
    Files.exists(path)
  } 

  def gamesParquetExists() : Boolean = {
    val path = Paths.get(gamesArchiveFile)
    Files.exists(path)
  }

  def createArchive () {
    val path = Paths.get(archivePath)
    Files.createDirectories(path)
  }

  def writeResultCSV(df:Dataset[GameDetail]) {
    df.write.mode(SaveMode.Overwrite).csv(gamesArchiveResult)
  }

  def createGamesParquet(df:Dataset[GameDetail]) = {
    df.write.mode(SaveMode.Overwrite).parquet(gamesArchiveFile)
  }

  def readGamesParquet(sparkSession: SparkSession) : Dataset[GameDetail] = {
    var df :Dataset[GameDetail] = null

    if(gamesParquetExists()) {
      df=sparkSession.read.parquet(gamesArchiveFile).as(Encoders.product[GameDetail]) 
    }

    df
  }
}
