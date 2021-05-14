package sparkML

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.VectorAssembler


object MachineLearning extends App {


  // usual spark session
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("AirBnBModel")
    .getOrCreate()


  // importing the data set
  val filePath = "src/main/scala/resources/chapter2/sf-airbnb/sf-airbnb-clean.parquet/"

  val airbnbDF = spark.read.parquet(filePath)

  airbnbDF.select("neighbourhood_cleansed", "room_type", "bedrooms", "bathrooms", "number_of_reviews", "price").show(5)

  // splitting the dataset and showing the number of records
  val Array(trainDF, testDF) = airbnbDF.randomSplit(Array(.8,.2), seed=42)
  println(f"""There are ${trainDF.count} rows in the training set, and ${testDF.count} in the test set""")

  // preparing feature for the ML model with transform()
  val vecAssembler = new VectorAssembler()
    .setInputCols(Array("bedrooms"))
    .setOutputCol("features")
  val vecTrainDF = vecAssembler.transform(trainDF)
  vecTrainDF.select("bedrooms","features","price").show(10)




}
