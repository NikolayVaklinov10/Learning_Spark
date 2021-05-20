package sparkML

import org.apache.spark.sql.SparkSession

object NetworkAnomalyDetection {

  def main(args: Array[String]): Unit = {

    // the usual spark session
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Anomaly Detection in Network traffic")
      .getOrCreate()

  }

}
