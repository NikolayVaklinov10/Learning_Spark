package externalDataSources

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}
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

  spark.sql("SELECT * FROM airports_na LIMIT 10").show()


  spark.sql("SELECT * FROM departureDelays LIMIT 10").show()

  spark.sql("SELECT * FROM foo").show()


  // Union two tables
  val bar = delays.union(foo)
  bar.createOrReplaceTempView("bar")
  bar.filter(expr("""origin == 'SEA' AND destination == 'SFO' AND date LIKE '0101010%' AND delay > 0 """)).show()

  spark.sql(
    """
      |SELECT *
      |FROM bar
      |WHERE origin = 'SEA'
      |   AND destination = 'SFO'
      |   AND date LIKE '01010%'
      |   AND delay > 0
      |""".stripMargin).show()

  // Join Departure Delays data (foo) with flight info
//  foo.join(
//    airport.as('air),
//    $"air.IATA" === $"origin"
//  ).select("City", "State", "date", "delay", "distance", "destination").show()

//  spark.sql("DROP TABLE IF EXISTS departureDelaysWindow")
//  spark.sql("""
//CREATE TABLE departureDelaysWindow AS
//SELECT origin, destination, sum(delay) as TotalDelays
//  FROM departureDelays
// WHERE origin IN ('SEA', 'SFO', 'JFK')
//   AND destination IN ('SEA', 'SFO', 'JFK', 'DEN', 'ORD', 'LAX', 'ATL')
// GROUP BY origin, destination
//""")
//
//  spark.sql("""SELECT * FROM departureDelaysWindow""").show()





}
