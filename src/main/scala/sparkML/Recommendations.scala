package sparkML

import org.apache.spark.sql.SparkSession

object Recommendations {

  def main(args: Array[String]): Unit = {

    // the usual spark session
    val spark  = SparkSession.builder()
      .master("local[*]")
      .appName("Audio Recommendation ML App")
      .getOrCreate()

    val rawUserArtistData = "src/main/scala/resources/chapter2/profiledata_06-May-2005/user_artist_data.txt"

    val rawUserArtistDataDF = spark.read.text(rawUserArtistData)

    rawUserArtistDataDF.take(5).foreach(println)





  }



}
