package sparkML

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

object Recommendations {


  def main(args: Array[String]): Unit = {

    // the usual spark session
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Audio Recommendation ML App")
      .getOrCreate()

    val rawUserArtistData = "src/main/scala/resources/chapter2/profiledata_06-May-2005/user_artist_data.txt"

    val rawUserArtistDataDF = spark.read.textFile(rawUserArtistData)

    //    rawUserArtistDataDF.take(5).foreach(println)


    // Note the following will fail
    import spark.implicits._
    rawUserArtistDataDF.map { line =>
      val (id, name) = line.span(_ != '\t')
      (id.toInt, name.trim)
    }.count()





  }
}
