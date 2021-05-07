import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.io.Source

object ch10Prj_ex2 extends App {
  
    Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "ch10Prj_ex2")
  val input00 = sc.textFile("C:/Users/Owner/Documents/Trendy_Tech/Week-10/Project_datasets/chapters-201108-004545.csv")
  // RDD for Chapter, Course
  //val courseChap = input01.map(_.split(",")).map(x=>(x(1), x(0))).groupByKey().map(x=>(x._1, x._2.toList))
  // Chapter, Course
  val courseChap = input00.map(_.split(","))
  
  val ccLookup = courseChap.map(x=>(x(0), x(1)))
  
  //RDD for Course and Number of Chapter
  val ccCount = courseChap.map(x=>(x(1),1)).reduceByKey(_+_) //.foreach(println)
  
  // RDD for User Chapter view files
  val input02 = sc.textFile("C:/Users/Owner/Documents/Trendy_Tech/Week-10/Project_datasets/views1-201108-004545.csv")

  // Add the values to the Log RDD
  //Chapter, User
  val rdd1 = input02.map(_.split(",")).map(x=>(x(1), x(0))).distinct()   
  
  //Join both the RDD's based on the Chapter ID
  // Chapter, (User, Course)
  val rdd2 = rdd1.join(ccLookup)
  
  //Change User to Key and replace the chapter id to 1 - To enable sum of the chapters of a given course viewed by the user
  // rdd3 = rdd2.map(x=>(x._2._1, (x._2._2, x._1))).take(20)
  
  // User, Course, Chapter (changed to 1)
  val rdd3 = rdd2.map(x=>((x._2._1, x._2._2), 1)) //.take(5).foreach(println)
  
  //Find the number of Chapters viewed by the User (User, Course) combination
  // ReduceByKey on (User, Course)
  val rdd4 = rdd3.reduceByKey(_+_) //.take(5).foreach(println)
 
  // Dropping the User ID as the Course and No of Chapter views are available now for every user
  val rdd5 = rdd4.map(x=>(x._1._2, x._2)) //.take(5).foreach(println)
  
  //Join it with number of chapters, course RDD from the previous chapter
  val rdd6 = rdd5.join(ccCount) //.collect.foreach(println)
  //Divide the actual number of chapters viewed/Total number of Chapters
  val rdd7 = rdd6.mapValues(x=>(x._1.toDouble/x._2)) //.collect.foreach(println)
  
  //Apply rules and find the score
  def score(perc: Double): Integer = {
      if (perc > 0.9) 10
      else if (perc > 0.5) 4
      else if (perc > 0.25) 2
      else 0
    }

  val rdd8 = rdd7.mapValues(score) //.collect.foreach(println)
  
  //Group the Courses and add the scores, sort it by Highest scores to Lowest
  val result = rdd8.reduceByKey(_+_).sortBy(_._2, false) //.collect.foreach(println)
  
  //Get the Course Title details to a RDD
  val fileRead = sc.textFile("C:/Users/Owner/Documents/Trendy_Tech/Week-10/Project_datasets/titles-201108-004545.csv")
  val courseTitle = fileRead.map(_.split(",")).map(x=> (x(0), x(1))) //.collect.foreach(println)
  
  //Display the final result
  println("Score,	Course Title")
  
  //Join with the Course Title RDD to obtain the Course Title
  val finalResult = result.join(courseTitle).map(x=>x._2).sortByKey()
  
  
  for(result <- finalResult) {
    val title = result._2
    val score = result._1
    println(s"$score	$title")
    }
   
}