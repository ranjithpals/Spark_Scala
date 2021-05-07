import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.io.Source

object ch10Project_ex1 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "ch10Project_ex1")
  val input01 = sc.textFile("C:/Users/Owner/Documents/Trendy_Tech/Week-10/Project_datasets/chapters-201108-004545.csv")
  // RDD for Course Chapter
  //val courseChap = input01.map(_.split(",")).map(x=>(x(1), x(0))).groupByKey().map(x=>(x._1, x._2.toList))
  val courseChap = input01.map(_.split(",")).map(x=>(x(1), x(0)))
  println(courseChap.lookup("125").toArray.toString())
  
  def findChapter(chapter: String): String = {
    val temp = courseChap.lookup(chapter).toArray
    temp(0).toString()
  }
  // RDD for User Chapter view files
  val input02 = sc.textFile("C:/Users/Owner/Documents/Trendy_Tech/Week-10/Project_datasets/views1-201108-004545.csv")
  //val input03 = sc.textFile("C:/Users/Owner/Documents/Trendy_Tech/Week-10/Project_datasets/views2-201108-004545.csv")
  //val input04 = sc.textFile("C:/Users/Owner/Documents/Trendy_Tech/Week-10/Project_datasets/views3-201108-004545.csv")
  // Combining data from all the files
  //val input05 = sc.union(input02, input03, input04)
  // Count of records in the combined files
  //println(input05.count())
  
  // Add the values to the Log RDD
  //val rdd2 = input02.map(_.split(",")).map(x=>(x(0), x(1))).map(x=>(x._1, (x._2, findChapter(x._2)))).take(5).foreach(println) //findChapter(x._2)
  
}