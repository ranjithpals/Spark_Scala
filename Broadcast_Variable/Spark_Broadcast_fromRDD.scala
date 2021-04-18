import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.io.StdIn

// Good Read on the Broadcast as an expensive operation due to collect action involved
// https://stackoverflow.com/questions/38329738/how-to-transform-rdd-dataframe-or-dataset-straight-to-a-broadcast-variable-with

// Find the average of Movie Ratings from Ratings file and display the average with the Movie Title

object Spark_Broadcast extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]", "broadcast")
  val input = sc.textFile("C:/Users/Owner/Documents/Trendy_Tech/Week-11/Datasets/movies-201019-002101.dat")
  
  val rdd1 = input.map(_.split("::")).map(x=>(x(0), x(1))).collect().toMap //.take(5).foreach(println)
  
  //Create a broadcast variable
  var bc = sc.broadcast(rdd1)
  
  val input01 = sc.textFile("C:/Users/Owner/Documents/Trendy_Tech/Week-11/Datasets/ratings-201019-002101.dat")
  
  val rdd2 = input01.map(_.split("::")).map(x=>(x(1), x(2))).mapValues(x=>(x.toFloat, 1.0))  //.take(5).foreach(println)
  val rdd3 = rdd2.reduceByKey((x,y) =>(x._1+y._1, x._2+y._2)).mapValues(x=>x._1/x._2) //.take(5).foreach(println)

  
  val finalResult = rdd3.map(x=>(bc.value(x._1),x._2)).collect
  

  for (result <- finalResult){
    val film = result._1
    val score = result._2
    println(s"$film, $score")
  }
  
  /*
  println(bc.value("2543"))
  for((k, v) <- bc.value){
    println(k)
  }
  */
}