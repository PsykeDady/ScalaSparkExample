package co.psyke

import co.psyke.utils.files.PersistUtility
import co.psyke.utils.http.APICalls
import co.psyke.config.SparkInit
import org.apache.spark.sql.{Dataset,SparkSession}
import co.psyke.mapping.GamesDataFrameFromJson
import org.apache.spark.sql.functions._; 
import co.psyke.models.GameDetail
import org.apache.spark.sql.Encoders


/**
 * @author ${user.name}
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + " " + b)
  
  def main(args : Array[String]) :Unit = {
    println( "Hello World!" )
    println("concat arguments = " + foo(args))
    
    var parquet :Dataset[GameDetail] = null
    var min : Double = Integer.MIN_VALUE 
    var max : Double = Integer.MAX_VALUE
    var name : String = ""
    
    var i=0; 
    while(i< args.length) {
      println(args(i))
      args(i) match {
        case "-m" | "--min-prezzo" => { 
          i=i+1
          min=args(i).toDouble
        }
        case "-M" |"--max-prezzo" => {
          i=i+1
          max=args(i).toDouble
        }
        case "-n" |"--name-contain" => {
          i=i+1
          name=args(i)
        }
      }
      i=i+1
      println(i)
    }
    
    val spark = SparkInit.getSparkSession

    if(!PersistUtility.archiveExists()) {
      PersistUtility.createArchive();
    }

    if(PersistUtility.gamesParquetExists()) {
      parquet=PersistUtility.readGamesParquet(spark)
    } else {
      // Il parquet non esiste, fare chiamata HTTP per creare JSON 
      val json = APICalls.apiGet("http://api.steampowered.com/ISteamApps/GetAppList/v0002/?format=json")
      parquet = GamesDataFrameFromJson.mapListGameToDF(json,spark)
      
    }

    val cachedParquet = parquet.cache(); 


    if(name!=null && name!=""){
      var risultato= cachedParquet.filter(col("name").contains(name)).as(Encoders.product[GameDetail])
      PersistUtility.writeResultCSV(risultato)
    }

    PersistUtility.createGamesParquet(parquet)
    spark.stop()
  }

}
