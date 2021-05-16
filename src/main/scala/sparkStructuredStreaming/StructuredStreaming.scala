package sparkStructuredStreaming

import org.apache.spark.sql.SparkSession

object StructuredStreaming extends App {

  // the spark session
  val spark = SparkSession.builder()
    .appName("The Streaming App")
    .master("local[*]")
    .getOrCreate()



}
