import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.io.Source

object Scala_AccumulatorVariable extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]", "Accumulator")
  
  //val accum = sc.longAccumulator("empty line accumulator")
  var accum = 0
  val input = sc.textFile("C:/Users/Owner/OneDrive/Documents/Big Data_TrendyTech/Week-10/Data/samplefile.txt")
  
  //input.foreach(x => if(x=="") accum.add(1))
  input.foreach(x=>if(x=="")accum += 1)
  
  //println(s"No of Blank lines in the file: $accum.value)")
  println(accum)
}