package dataHouseWithDeltaLake

import org.apache.spark.sql.SparkSession

object DeltaLake extends App {

  // Configure source data
  val sourcePath = "src/main/scala/resources/chapter2/loans/loan-risks.snappy.parquet"

  // Configure Delta Lake path
  val deltaPath = "/tmp/loans_data"

  // the usual  spark session
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("DeltaLake")
    .getOrCreate()

  // Create the Delta table with the same loans data
  spark.read
    .format("parquet")
    .load(sourcePath)
    .write
    .format("delta")
    .save(deltaPath)

  // Create a view on the delta called loans_delta
  spark.read
    .format("delta")
    .load(deltaPath)
    .createOrReplaceTempView("loans_delta")

  // Loans row count
  spark.sql("SELECT count(*) FROM loans_delta").show()




}
