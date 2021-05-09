package externalDataSources

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object Flights extends App {

  // Set file paths
  val delaysPath = "src/main/scala/resources/chapter2/flights/departuredelays.csv"
  val airportPath = "src/main/scala/resources/chapter2/flights/airport-codes-na.txt"


  // the spark session bit
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("FlightsApp")
    .getOrCreate()

  // Obtain airport data set
  val airport = spark.read
    .option("header", true)
    .option("inferSchema", true)
    .option("delimiter", "\t")
    .csv(airportPath)
  airport.createOrReplaceTempView("airports_na")

  // Obtain departure Delays data set
  val delays = spark.read
    .option("header", true)
    .csv(delaysPath)
    .withColumn("delay", expr("CAST(delay as INT) as delay"))
    .withColumn("distance", expr("CAST(distance as INT) as distance"))
  delays.createOrReplaceTempView("departureDelays")

  // Create temporary small table
  val foo = delays.filter(
    expr(
      """origin == 'SEA' AND destination == 'SFO' AND
        | date like '01010%' AND delay > 0 """.stripMargin))
  foo.createOrReplaceTempView("foo")

}
