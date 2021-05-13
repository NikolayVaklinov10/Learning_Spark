package sparkML

import org.apache.spark.sql.SparkSession

object MachineLearning extends App {


  // usual spark session
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("AirBnBModel")
    .getOrCreate()


  // importing the data set
  val filePath = "src/main/scala/resources/chapter2/sf-airbnb/sf-airbnb-clean.parquet/"

  val airbnb = spark.read.parquet(filePath)

  airbnb
    .select("neighbourhood_cleansed",
      "room_type", "bedrooms", "bathrooms", "number_of_reviews", "price").show(5)
}
