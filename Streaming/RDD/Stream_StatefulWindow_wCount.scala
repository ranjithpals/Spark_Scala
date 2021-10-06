import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds


object Stream_StatefulWindow_wCount extends App {
  
  //Set Log level 
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  //Create Spark Context
  val sc = new SparkContext("local[*]", "Streaming Word Count")
  
  //Create a Spark Streaming context
  val ssc = new StreamingContext(sc, Seconds(2))
  
  //Read the Stream from the Producer: nc -lk 9994
  val baseRDD = ssc.socketTextStream("g01.itversity.com", 9994)
  
  //Apply transformation to flatten the Input
  val rdd1 = baseRDD.flatMap(x=>x.split(" "))
  
  //Apply Transformation to convert each word to key-value pair
  val rdd2 = rdd1.map(x=>(x, 1))
  
  ssc.checkpoint(".")
  
  def updateFunc(newValues: Seq[Int], previousState: Option[Int]): Option[Int] = {
    //Adding the previous state's value
    val newCount = previousState.getOrElse(0) + newValues.sum
    Some(newCount)
  }
  
  //Count the occurrences of each word using Stateful Window and Sliding Interval
  val finalRDD = rdd2.reduceByKeyAndWindow((x, y)=>x+y, (x, y)=>x-y, Seconds(8), Seconds(4))
  
  //Display the word count
  finalRDD.print()
  
  //Start listening to the stream
  ssc.start()
  
  //Await Termination
  ssc.awaitTermination()
  
}