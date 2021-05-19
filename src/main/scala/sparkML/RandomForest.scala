package sparkML

import org.apache.spark.sql.SparkSession

object RandomForest {

  def main(args: Array[String]): Unit = {
    // spark session
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Random Forest Algorithm")
      .getOrCreate()

  }

}
