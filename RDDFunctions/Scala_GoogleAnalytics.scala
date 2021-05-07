import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Scala_GoogleAnalytics extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]", "googleAnalytics")
  
  val input = sc.textFile("C:/Users/Owner/OneDrive/Documents/Big Data_TrendyTech/Week-10/Data/bigdatacampaigndata.csv")
  
  // Sample data from the csv file
  //big data contents,	Broad match,	None,	TrendyTech Search India,	Broad Match #3,	1,	1,	100%,	INR,	24.06,	24.06,	0,	0,	0%,	Search
  // Split each line by " " to make an array of elements
  val rdd1 = input.map(_.split(","))
  // extract only the columns of interest.
  // Split the value by " " and map the key and value and combine such pairs from all the lines
  // reorder the key and value (k, v) => (v, k)
  // reduce by key - add the values for same key
  // sort by value - descending order
  val rdd2 = rdd1.map(x=>(x(10).toFloat, x(0))).flatMapValues(x=>x.split(" ")).map(x=>(x._2, x._1)).reduceByKey(_+_).sortBy(_._2, false)
  // print the tuple - first 5
  rdd2.take(5).foreach(println)
  
}