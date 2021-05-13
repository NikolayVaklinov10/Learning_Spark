package sparkML

import org.apache.spark.sql.SparkSession

object MachineLearning extends App {


  // usual spark session
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("AirBnBModel")
    .getOrCreate()

}
