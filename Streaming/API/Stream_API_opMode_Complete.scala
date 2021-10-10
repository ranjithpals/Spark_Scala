import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Seconds


object Stream_API_opMode_Complete extends App {
  
  //Set Logger Level
  Logger.getLogger("org").setLevel(Level.INFO)
  
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
            
  //Transform the string messages using explode function
  //to convert each word to row
  val transformDF = df1.selectExpr("explode(split(value, ' ')) as rows")
                    .groupBy("rows").count()
            
  //Write the Streaming data
  val writeDf = transformDF.writeStream
                .format("console")
                .outputMode("complete")
                .option("checkpointLocation", "checkPoint-Loc1")
                .start()
                
  writeDf.awaitTermination() 
    
}