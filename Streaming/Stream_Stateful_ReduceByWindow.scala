import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds


object Stream_Stateful_ReduceByWindow extends App {
  
  //Set Log level 
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  //Create Spark Context
  val sc = new SparkContext("local[*]", "Streaming Word Count")
  
  //Create a Spark Streaming context
  val ssc = new StreamingContext(sc, Seconds(2))
  
  //Create a initial Checkpoint
  ssc.checkpoint(".")
  
  //Read the Stream from the Producer: nc -lk 9994
  val lines = ssc.socketTextStream("g01.itversity.com", 9994)
  
  def summaryFunc(x: String, y: String): String = {
     (x.toInt + y.toInt).toString
  }
  
  def InverseFunc(x: String, y: String): String = {
     (x.toInt - y.toInt).toString
  }
    
  //Count the occurrences of each word using Stateful Window and Sliding Interval
  val finalRDD = lines.reduceByWindow(summaryFunc, InverseFunc, Seconds(8), Seconds(4))
  
  //Display the word count
  finalRDD.print()
  
  //Start listening to the stream
  ssc.start()
  
  //Await Termination
  ssc.awaitTermination()
  
}