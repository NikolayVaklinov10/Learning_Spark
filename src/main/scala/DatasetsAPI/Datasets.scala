package DatasetsAPI

object Datasets extends App {

  // making the case class needed for the schema of the dataset
  case class Bloggers(id:Int, first:String, last:String, url:String, date:String, hits: Int, campaigns:Array[String])




}
