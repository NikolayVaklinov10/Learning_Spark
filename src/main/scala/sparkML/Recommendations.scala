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


    val rawUserArtistData = spark.read.textFile("src/main/scala/resources/chapter2/profiledata_06-May-2005/user_artist_data.txt")

//        rawUserArtistData.take(5).foreach(println)

    // the following import is required
    import spark.implicits._
    // a little change of the structure of the data
    val userArtistDF = rawUserArtistData.map{ line =>
      val Array(user, artist, _*) = line.split(' ')
      (user.toInt, artist.toInt)
    }.toDF("user", "artist")






  }
}
