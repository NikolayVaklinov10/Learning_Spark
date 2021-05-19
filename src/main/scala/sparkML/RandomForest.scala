package sparkML

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
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

      // check the importance of each feature to the prediction capabilities of the model
      model.featureImportances.toArray.zip(inputCols).
        sorted.reverse.foreach(println)

      val predictions = model.transform(assembledTrainData)

      predictions.select("Cover_Type", "prediction", "probability").
        show(truncate = false)

      val evaluator = new MulticlassClassificationEvaluator().
        setLabelCol("Cover_Type").
        setPredictionCol("prediction")

      val accuracy = evaluator.setMetricName("accuracy").evaluate(predictions)
      val f1 = evaluator.setMetricName("f1").evaluate(predictions)
      println(accuracy)
      println(f1)

      // the following compute the confusion matrix
      val predictionRDD = predictions.
        select("prediction", "Cover_Type").
        as[(Double,Double)].rdd
      val multiclassMetrics = new MulticlassMetrics(predictionRDD)
      println(multiclassMetrics.confusionMatrix)

      // the DataFrame version of the above code
      val confusionMatrix = predictions.
        groupBy("Cover_Type").
        pivot("prediction", (1 to 7)).
        count().
        na.fill(0.0).
        orderBy("Cover_Type")

      confusionMatrix.show()
    }

    // evaluating the accuracy
    def classProbabilities(data: DataFrame): Array[Double] = {
      val total = data.count()
      data.groupBy("Cover_Type").count()
        .orderBy("Cover_Type")
        .select("count").as[Double]
        .map(_ / total)
        .collect()
    }

    def randomClassifier(trainData: DataFrame, testData: DataFrame): Unit = {
      val trainPriorProbabilities = classProbabilities(trainData)
      val testPriorProbabilities = classProbabilities(testData)
      val accuracy = trainPriorProbabilities.zip(testPriorProbabilities).map {
        case (trainProb, cvProb) => trainProb * cvProb
      }.sum
      println(accuracy)
    }

    def evaluate(trainData: DataFrame, testData: DataFrame): Unit = {

      val inputCols = trainData.columns.filter(_ != "Cover_Type")
      val assembler = new VectorAssembler().
        setInputCols(inputCols).
        setOutputCol("featureVector")

      val classifier = new DecisionTreeClassifier().
        setSeed(Random.nextLong()).
        setLabelCol("Cover_Type").
        setFeaturesCol("featureVector").
        setPredictionCol("prediction")

      val pipeline = new Pipeline().setStages(Array(assembler, classifier))

      // the built-in support for testing hyperparameters combination
      val paramGrid = new ParamGridBuilder().
        addGrid(classifier.impurity, Seq("gini", "entropy")).
        addGrid(classifier.maxDepth, Seq(1, 20)).
        addGrid(classifier.maxBins, Seq(40, 300)).
        addGrid(classifier.minInfoGain, Seq(0.0, 0.05)).
        build()

      val multiclassEval = new MulticlassClassificationEvaluator().
        setLabelCol("Cover_Type").
        setPredictionCol("prediction").
        setMetricName("accuracy")

      // bringing the components together
      val validator = new TrainValidationSplit().
        setSeed(Random.nextLong()).
        setEstimator(pipeline).
        setEvaluator(multiclassEval).
        setEstimatorParamMaps(paramGrid).
        setTrainRatio(0.9)

      val validatorModel = validator.fit(trainData)

      val paramsAndMetrics = validatorModel.validationMetrics.
        zip(validatorModel.getEstimatorParamMaps).sortBy(-_._1)

      paramsAndMetrics.foreach { case (metric, params) =>
        println(metric)
        println(params)
        println()
      }

      val bestModel = validatorModel.bestModel

      println(bestModel.asInstanceOf[PipelineModel].stages.last.extractParamMap)

      // the accuracy achieved on CV and Test data
      println(validatorModel.validationMetrics.max)

      val testAccuracy = multiclassEval.evaluate(bestModel.transform(testData))
      println(testAccuracy)


    }



  }

}
