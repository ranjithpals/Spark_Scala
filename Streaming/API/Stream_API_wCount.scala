import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Seconds


object Stream_API_wCount extends App {
  
  //Set Logger Level
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  //Create Spark Session
  val spark = SparkSession.builder()
              .appName("Streaming API WordCount")
              .master("local[*]")
              .getOrCreate()
              
  //Read Streaming data from socket
  val df1 = spark.readStream
            .format("socket")
            .option("host", "g01.itversity.com")
            .option("port", 9990)
            .load()
            
  //Write the Streaming data
  val writeDf = df1.writeStream
                .format("console")
                .outputMode("append")
                .start()
                
  writeDf.awaitTermination() 
    
}