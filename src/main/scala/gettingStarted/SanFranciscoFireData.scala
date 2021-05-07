package gettingStarted

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{BooleanType, FloatType, IntegerType, StringType, StructField, StructType}

object SanFranciscoFireData extends App {

  // the spark session for the code
  val spark = SparkSession.builder()
    .appName("SanFranciscoData")
    .master("local[*]")
    .getOrCreate()

  // the schema bit
  val fireSchema = StructType(Array(
    StructField("CallNumber", IntegerType, true),
    StructField("UnitID", StringType, true),
    StructField("IncidentNumber", IntegerType, true),
    StructField("CallType", StringType, true),
    StructField("CallDate", StringType, true),
    StructField("WatchDate", StringType, true),
    StructField("CallFinalDisposition", StringType, true),
    StructField("AvailableDtTm", StringType, true),
    StructField("Address", StringType, true),
    StructField("City", StringType, true),
    StructField("Zipcode", IntegerType, true),
    StructField("Battalion", StringType, true),
    StructField("StationArea", StringType, true),
    StructField("Box", StringType, true),
    StructField("OriginalPriority", StringType, true),
    StructField("Priority", StringType, true),
    StructField("FinalPriority", IntegerType, true),
    StructField("ALSUnit", BooleanType, true),
    StructField("CallTypeGroup", StringType, true),
    StructField("NumAlarms", IntegerType, true),
    StructField("UnitType", StringType, true),
    StructField("UnitSequenceInCallDispatch", IntegerType, true),
    StructField("FirePreventionDistrict", StringType, true),
    StructField("SupervisorDistrict", StringType, true),
    StructField("Neighborhood", StringType, true),
    StructField("Location", StringType, true),
    StructField("RowID", StringType, true),
    StructField("Delay", FloatType, true),
  ))

  val sfFireFile = "src/main/scala/resources/chapter2/sf-fire/sf-fire-calls.csv"
  val fireDF = spark.read.schema(fireSchema)
    .option("header", true)
    .csv(sfFireFile)
//  fireDF.show(10)



  // projection and filtering
  val fewFireDF = fireDF
    .select("IncidentNumber", "AvailableDtTm", "CallType")
    .where(col("CallType") =!= "Medical Incident")
    .show(5, false)



}
