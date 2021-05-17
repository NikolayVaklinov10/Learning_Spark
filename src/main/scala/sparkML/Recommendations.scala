package sparkML

import org.apache.spark.sql.SparkSession

object Recommendations {

  def main(args: Array[String]): Unit = {

    // the usual spark session
    val spark  = SparkSession.builder()
      .master("local[*]")
      .appName("Audio Recommendation ML App")
      .getOrCreate()





  }



}
