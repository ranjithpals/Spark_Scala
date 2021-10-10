import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.streaming.Trigger


object Stream_API_opMode_Complete_Configs extends App {
  
  //Set Logger Level
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  //Create Spark Session
  val spark = SparkSession.builder()
              .appName("Streaming API WordCount with Configs")
              .master("local[*]")
              .config("spark.sql.shuffle.partitions", 3)
              .config("spark.streaming.stopGracefullyOnShutdown", "true")
              .getOrCreate()
                         
  //Read Streaming data from socket
  val df1 = spark.readStream
            .format("socket")
            .option("host", "g01.itversity.com")
            .option("port", 12345)
            .load()
            
  //Transform the string messages using explode function
  //to convert each word to row
  val transformDF = df1.selectExpr("explode(split(value, ' ')) as rows")
                    .groupBy("rows").count()
            
  //Write the Streaming data
  val writeDf = transformDF.writeStream
                .format("console")
                .outputMode("complete")
                .option("checkpointLocation", "checkPoint-Loc1")
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .start()
                
  writeDf.awaitTermination() 
    
}