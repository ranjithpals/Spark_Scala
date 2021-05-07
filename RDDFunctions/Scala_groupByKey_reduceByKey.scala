import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Scala_groupByKey_reduceByKey extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "groupByKey_reduceByKey")
  val input = sc.textFile("C:/Users/Owner/OneDrive/Documents/Big Data_TrendyTech/Week-10/Data/bigLog.txt")
  
  val rdd1 = input.map(_.split(':')).map(x=>(x(0), 1))
  
  //Using GroupByKey
  //val rdd2 = rdd1.groupByKey().collect().foreach(x=>println(x._1, x._2.size))
  
  //Using ReduceByKey
  val rdd2 = rdd1.reduceByKey(_+_).collect().foreach(println)
  
  scala.io.StdIn.readLine()
}