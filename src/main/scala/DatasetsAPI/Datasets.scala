package DatasetsAPI

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc

object Datasets extends App {

  // making the case class needed for the schema of the dataset
  case class Bloggers(id:Int, first:String, last:String, url:String, date:String, hits: Int, campaigns:Array[String])

  // the usual spark session
  val spark = SparkSession.builder()
    .appName("BloggerApp")
    .master("local[*]")
    .getOrCreate()


  // initializing a value for the blogger
  val bloggers = "src/main/scala/resources/chapter2/blogs.json"

  // the implicits needed
//  import spark.implicits._
  // making the dataset
  val bloggersDS = spark.read
    .format("json")
    .option("path", bloggers)
//    .load().as[Bloggers]


  // Usage dataset generated data
  import scala.util.Random._
  // the case class for the dataset
  case class Usage(uid: Int, uname: String, usage: Int)
  val r = new scala.util.Random(42)
  // Create 1000 instances of scala Usage class
  // This generates data on the fly
  val data = for (i <- 0 to 1000)
    yield (Usage(i, "user-" + r.alphanumeric.take(5).mkString(""),
      r.nextInt(1000)))
  // Create a Dataset of Usage typed data
  import spark.implicits._
  val dsUsage = spark.createDataset(data)
//  dsUsage.show(10)

  // higher-oder functions
  dsUsage.filter(d => d.usage > 900)
    .orderBy(desc("usage"))
    .show(5, false)


}
