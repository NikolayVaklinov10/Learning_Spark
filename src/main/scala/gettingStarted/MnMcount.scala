package gettingStarted

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc}

object MnMcount extends App {

  val spark = SparkSession.builder()
    .appName("MnMCount")
    .master("local[*]")
    .getOrCreate()

  val mnm = spark.read
    .option("inferSchema", true)
    .csv("src/main/scala/resources/chapter2/mnm_dataset.csv")

  // first 10 rows of csv
  val mnmText = spark.read
    .option("inferSchema", true)
    .text("src/main/scala/resources/chapter2/README.md")

  // the first 10 rows of a text
  mnmText.show(10, false)



}
