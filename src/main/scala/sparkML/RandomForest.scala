package sparkML

import org.apache.spark.sql.SparkSession

object RandomForest {

  def main(args: Array[String]): Unit = {
    // spark session
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Random Forest Algorithm")
      .getOrCreate()

    import spark.implicits._

    val dataWithoutHeader = spark.read
      .option("inferSchema", true)
      .option("header", false)
      .csv("src/main/scala/resources/chapter2/covtype.data")


    val colNames = Seq(
      "Elevation", "Aspect", "Slope",
      "Horizontal_Distance_To_Hydrology", "Vertical_Distance_To_Hydrology",
      "Horizontal_Distance_To_Roadways",
      "Hillshade_9am", "Hillshade_Noon", "Hillshade_3pm",
      "Horizontal_Distance_To_Fire_Points"
    ) ++ (
      (0 until 4).map(i => s"Wilderness_Area_$i")
      ) ++ (
      (0 until 40).map(i => s"Soil_Type_$i")
      ) ++ Seq("Cover_Type")

    val data = dataWithoutHeader.toDF(colNames:_*).
      withColumn("Cover_Type", $"Cover_Type".cast("double"))

    data.show()
    data.head

    // Split into 90% train (+ CV), 10% test
    val Array(trainData, testData) = data.randomSplit(Array(0.9, 0.1))
    trainData.cache()
    testData.cache()

  }

}
