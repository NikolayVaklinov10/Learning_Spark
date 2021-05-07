package gettingStarted

import org.apache.spark.sql.SparkSession

object SanFranciscoFireData extends App {

  // the spark session for the code
  val spark = SparkSession.builder()
    .appName("SanFranciscoData")
    .master("local[*]")
    .getOrCreate()



}
