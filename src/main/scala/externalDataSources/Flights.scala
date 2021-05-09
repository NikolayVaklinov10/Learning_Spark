package externalDataSources

import org.apache.spark.sql.SparkSession

object Flights extends App {

  // Set file paths
  val delaysPath = "src/main/scala/resources/chapter2/flights/departuredelays.csv"
  val airportPath = "src/main/scala/resources/chapter2/flights/airport-codes-na.txt"


  // the spark session bit
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("FlightsApp")
    .getOrCreate()


}
