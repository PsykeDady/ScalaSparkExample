package co.psyke.mapping

import org.apache.spark.sql.{Encoders,SparkSession,Dataset}
import org.apache.spark.sql.functions.{explode,col}; 
import co.psyke.models.GameDetail

object  GamesDataFrameFromJson {
    def mapListGameToDF(json:String, spark:SparkSession): Dataset[GameDetail] = {
        import spark.implicits._ //serve per avere toDS
        val parquet = spark.read.json(Seq(json).toDS)
        parquet.select(explode(col("applist.apps")) as "app").select("app.appid","app.name").as(Encoders.product[GameDetail])
    }
}
