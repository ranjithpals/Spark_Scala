import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds


object StreamingWordCount extends App {
  
  //Set Log level 
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  //Create Spark Context
  val sc = new SparkContext("local[*]", "Streaming Word Count")
  
  //Create a Spark Streaming context
  val ssc = new StreamingContext(sc, Seconds(5))
  
  //Read the Stream from the Producer: nc -lk 9995
  val baseRDD = ssc.socketTextStream("g01.itversity.com", 9995)
  
  //Apply transformation to flatten the Input
  val rdd1 = baseRDD.flatMap(x=>x.split(" "))
  
  //Apply Transformation to convert each word to key-value pair
  val rdd2 = rdd1.map(x=>(x, 1))
  
  //Count the occurrences of each word
  val finalRDD = rdd2.reduceByKey(_+_)
  
  //Display the word count
  finalRDD.print()
  
  //Start listening to the stream
  ssc.start()
  
  //Await Termination
  ssc.awaitTermination()
  
}