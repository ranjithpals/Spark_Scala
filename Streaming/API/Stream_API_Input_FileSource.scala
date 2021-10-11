import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Seconds


object Stream_API_Input_FileSource extends App {
  
  //Set Logger Level
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  //Create Spark Session with configuration to shutdown gracefully
  val spark = SparkSession.builder()
              .appName("Streaming API Input FileSource")
              .master("local[*]")
              .config("spark.sql.streaming.schemaInference", "true")
              .config("spark.sql.shuffle.partitions", 3)
              .config("spark.streaming.stopGracefullyOnShutdown", "true")
              .getOrCreate()
              
  //Read Streaming data from a Folder/Directory when new file is placed and 
  //only one file is picked per batch
  val df1 = spark.readStream
            .format("json")
            .option("path", "InputFolder")
            .option("maxFilesPerTrigger", 1)
            .load()
            
  //Create a View of the Input DataFrame
  df1.createOrReplaceTempView("orders")
            
  //Filter only the transactions with status COMPLETE
  val completedOrders = spark.sql("SELECT * FROM orders WHERE order_status='COMPLETE'")          
            
  //Write the Streaming data to file of JSON format in the Output Folder/Directory
  val writeDf = completedOrders.writeStream
                .format("json")
                .outputMode("append")
                .option("path", "OutputFolder")
                .option("checkpointLocation", "checkpoint-Loc2")
                .start()
                
  writeDf.awaitTermination() 
    
}