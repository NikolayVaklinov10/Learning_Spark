package builtinDataSources

import org.apache.spark.sql.SparkSession

object BasicQuery extends App {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("SparkSQLExampleApp")
    .getOrCreate()

  // the path to the data set
  val csvFile = "src/main/scala/resources/chapter2/flights/departuredelays.csv"

  // Read and create a temporary view
  // Infer schema
  val df = spark.read.format("csv")
    .option("inferSchema", true)
    .option("header", true)
    .load(csvFile)
  // Create a temporary view
  df.createOrReplaceTempView("us_delay_flights_tbl")

//  spark.sql(
//    """
//      |SELECT distance, origin, destination
//      |FROM us_delay_flights_tbl WHERE distance > 1000
//      |ORDER BY distance DESC
//      |""".stripMargin).show(10)

  // Find all flights with more than 2 hour delay between Chicago(ORD) and San Francisco (SFO)
  spark.sql(
    """
      |SELECT date, delay, origin, destination
      |FROM us_delay_flights_tbl
      |WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
      |ORDER BY delay DESC
      |""".stripMargin).show(10)


}