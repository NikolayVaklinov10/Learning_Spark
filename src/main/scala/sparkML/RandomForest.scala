package sparkML

import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Random

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

  class RunRDF(private val spark: SparkSession) {

    import spark.implicits._

    def simpleDecisionTree(trainData: DataFrame, testData: DataFrame): Unit = {

      // all features at one place
      val inputCols = trainData.columns.filter(_ != "Cover_Type")
      val assembler = new VectorAssembler()
        .setInputCols(inputCols)
        .setOutputCol("featureVector")

      val assembledTrainData = assembler.transform(trainData)
      assembledTrainData.select("featureVector").show(truncate = false)

      // the DecisionTree model classifier
      val classifier = new DecisionTreeClassifier()
        .setSeed(Random.nextLong())
        .setLabelCol("Cover_Type")
        .setFeaturesCol("featureVector")
        .setPredictionCol("prediction")

      val model = classifier.fit(assembledTrainData)
      println(model.toDebugString)




    }

  }

}
