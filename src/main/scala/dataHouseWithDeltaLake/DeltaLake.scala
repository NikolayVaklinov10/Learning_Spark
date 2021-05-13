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





}
