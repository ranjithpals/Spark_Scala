import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Scala_Ch10_prac1 extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]", "Chapter10_prac1")
  
  val myList = List("WARN: Tuesday 4 September 0405",
                    "ERROR: Tuesday 4 September 0408",
                    "ERROR: Tuesday 4 September 0409",
                    "ERROR: Tuesday 4 September 0410",
                    "ERROR: Tuesday 4 September 0410")
  
  //Find the number of times each warning levels appear in the log file
  val myRDD = sc.parallelize(myList)
  
  val finalResult = myRDD.map(x=> (x.split(":")(0), 1)).reduceByKey(_+_).collect()
  
  finalResult.foreach(println)
  
  //scala.io.StdIn.readLine()
}
