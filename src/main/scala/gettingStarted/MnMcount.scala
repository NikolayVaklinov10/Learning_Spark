package gettingStarted

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc}

object MnMcount extends App {

  // setting the spark session
  val spark = SparkSession.builder()
    .appName("MnMCount")
    .master("local[*]")
    .getOrCreate()

  // first 10 rows of csv
  val mnm = spark.read
    .option("header", true)
    .option("inferSchema", true)
    .csv("src/main/scala/resources/chapter2/mnm_dataset.csv")


  val mnmText = spark.read
    .option("inferSchema", true)
    .text("src/main/scala/resources/chapter2/README.md")

  // the first 10 rows of a text
  mnm.show(10, false)



}
