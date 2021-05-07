package gettingStarted

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, desc}

object MnMcount extends App {

  // setting the spark session
  val spark = SparkSession.builder()
    .appName("MnMCount")
    .master("local[*]")
    .getOrCreate()

  // first 10 rows of csv
  val mnmDF = spark.read
    .option("header", true)
    .option("inferSchema", true)
    .csv("src/main/scala/resources/chapter2/mnm_dataset.csv")


  val mnmText = spark.read
    .option("inferSchema", true)
    .text("src/main/scala/resources/chapter2/README.md")

  // the first 10 rows of a text
//  mnmDF.show(10, false)
  //Aggregate counts of all colors and groupBy() State and Color orderBy() in descending order
  val countMnMDF = mnmDF
  .select("State", "Color", "Count")
  .groupBy("State", "Color")
  .agg(count("Count").alias("Total"))
  .orderBy(desc("Total"))
  // Show the aggregate results for all states and colors
  countMnMDF.show(60)
  println(s"Total Rows = ${countMnMDF.count()}")


}
