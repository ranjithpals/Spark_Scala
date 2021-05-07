import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.io.Source

object ch10Prj_ex1 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "ch10Prj_ex1")
  val input = sc.textFile("C:/Users/Owner/Documents/Trendy_Tech/Week-10/Project_datasets/chapters-201108-004545.csv")
  println("CourseId, No.Chapters")
  val finalresult = input.map(_.split(",")).map(x=>(x(1), x(0))).countByKey().toSeq.sortBy(_._2)
  //Print the Results
  for(result <- finalresult){
    val chap = result._1
    val number = result._2
    println(s"$chap  	$number")}
  
  // Add Values of Pair RDD to a List, convert to a True Key-Value pair
  val addValtoList = input.map(_.split(",")).map(x=>(x(1), x(0))).groupByKey()
  //Print the Results
  for(result <- addValtoList){
    val chap = result._1
    val number = result._2.toList.sortBy(x=>x) // sortBy is optional, and the List works without sorting. toList converts CompactBuffer to List
    println(s"$chap  	$number")}
  
  //https://stackoverflow.com/questions/49452800/how-to-transform-a-compactbuffer-to-list
    
}