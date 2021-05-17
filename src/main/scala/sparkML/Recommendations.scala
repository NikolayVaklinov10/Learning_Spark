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

    // the following code querying through a couple of millions of records
    userArtistDF.agg(
      min("user"), max("user"), min("artist"), max("artist")).show()

    val rawArtistData = spark.read.textFile("src/main/scala/resources/chapter2/profiledata_06-May-2005/artist_data.txt")

//    // the following will fail due to map function
//    rawArtistData.map { line =>
//      val (id, name) = line.span(_ != '\t')
//      (id.toInt, name.trim)
//    }.count()

    // Better version
    val artistByID = rawArtistData.flatMap { line =>
      val (id, name) = line.span(_ != '\t')
      if (name.isEmpty) {
        None
      } else {
        try {
          Some((id.toInt, name.trim))
        } catch {
          case _: NumberFormatException => None
        }
      }
    }.toDF("id", "name")






  }
}
