import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.io.Source

object Scala_GAwBroadcastVariable extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]", "googleAnalytics")
  
  val input = sc.textFile("C:/Users/Owner/OneDrive/Documents/Big Data_TrendyTech/Week-10/Data/bigdatacampaigndata.csv")
  
  def createBoringWords():Set[String] = {
    // Create a variable to add the boring words from File
    var boringWords: Set[String] = Set()
    //load the list of boring words from local drive
    val lines = Source.fromFile("C:/Users/Owner/OneDrive/Documents/Big Data_TrendyTech/Week-10/Data/boringwords.txt").getLines()
    // Load the values from file
    for(line<-lines){
      boringWords += line
    }
   boringWords
  }
  
  //Create the Broadcast variable
  var nameSet = sc.broadcast(createBoringWords)
  
  // Sample data from the csv file
  //big data contents,	Broad match,	None,	TrendyTech Search India,	Broad Match #3,	1,	1,	100%,	INR,	24.06,	24.06,	0,	0,	0%,	Search
  // Split each line by " " to make an array of elements
  val rdd1 = input.map(_.split(","))
  // extract only the columns of interest.
  // Split the value by " " and map the key and value and combine such pairs from all the lines
  // reorder the key and value (k, v) => (v, k)
  val rdd2 = rdd1.map(x=>(x(10).toFloat, x(0))).flatMapValues(x=>x.split(" ")).map(x=>(x._2.toLowerCase(), x._1))
  // Filter the boring words
  val rdd3 = rdd2.filter(x => !nameSet.value(x._1))
  // reduce by key - add the values for same key
  // sort by value - descending order
  val rdd4 = rdd3.reduceByKey(_+_).sortBy(_._2, false)
  // print the tuple - first 5
  rdd4.take(5).foreach(println)
  
}