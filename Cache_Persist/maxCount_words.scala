import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object maxCount_words extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]", "wordcount")
  
  val input = sc.textFile("C:/Users/Owner/Documents/Trendy_Tech/Week-9/Data/search_data.txt")
  //Split text to array of words
  val rdd1 = input.flatMap(_.split(" "))
  //convert all text to lower
  val rdd2 = rdd1.map(_.toLowerCase())
  //Map each word in the Array to counter 1
  val rdd3 = rdd2.map((_, 1))
  //Group and Aggregate (Sum) of the distinct keys
  val rdd4 = rdd3.reduceByKey(_+_)
  //Sort the Tuple (Key, Value) by Values
  val finalResult = rdd4.sortBy(_._2,false)
  //Cache rdd3 with Map functions
  finalResult.cache()
  var results = finalResult.collect
  
  for(result <- finalResult){
    val word = result._1
    val count = result._2
    println(s"$word: $count")
  }
  
  scala.io.StdIn.readLine()
}