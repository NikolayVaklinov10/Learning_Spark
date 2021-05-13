package dataHouseWithDeltaLake

object DeltaLake extends App {

  // Configure source data
  val sourcePath = "src/main/scala/resources/chapter2/loans/loan-risks.snappy.parquet"

  // Configure Delta Lake path
  val deltaPath = "/tmp/loans_data"



}
