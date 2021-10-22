import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.parquet.format.TimeType

object Stream_API_Join_2_data_Streams extends App {
  
  //Set the Log Level
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  //Create Spark Streaming Session
  val strSession = SparkSession.builder()
                   .appName("Stream_API_Join_2_data_Streams")
                   .master("local[2]")
                   .config("spark.sql.shuffle.partitions", 4)
                   .config("spark.streaming.stopGracefullyOnShutdown", "true")
                   .getOrCreate()
                   
  //Read the Streaming data on Impressions while searching on a search engine
  val str1df = strSession.readStream
                .format("socket")
                .option("host", "g01.itversity.com")
                .option("port", 12341)
                .load()
                
  //Define Schema by constructing a StructType
  val impressionSchema = StructType(List(
                          StructField("impressionID", StringType),
                          StructField("ImpressionTime", TimestampType),
                          StructField("CampaignName", StringType)))
  
  //Read the Streaming data on Clicks on results from an search engine
  val str2df = strSession.readStream
                .format("socket")
                .option("host", "g01.itversity.com")
                .option("port", 12342)
                .load()
                
  //Define Schema by constructing a StructType
  val clickSchema = StructType(List(
                          StructField("clickID", StringType),
                          StructField("ClickTime", TimestampType)))
                     
  //Extract the text added to Console - contained within default column "value" of the DataFrame
  // from_json method converts the text to JSON of defined Schema 
  // and places it under column name "impression_val"
  val value1DF = str1df.select(from_json(col("value"), impressionSchema).alias("impression_val"))
  
  val value2DF = str2df.select(from_json(col("value"), clickSchema).alias("click_val"))
  //valueDF.printSchema()
  
  //Selects all the tags within the JSON as individual columns under the DataFrame and add a WaterMark
  val impressionDF = value1DF.select("impression_val.*").withWatermark("ImpressionTime", "30 minute")
  
  //Selects all the tags within the JSON as individual columns under the DataFrame and add a WaterMark
  val clickDF = value2DF.select("click_val.*").withWatermark("ClickTime", "30 minute")
                
  //Join both the Streaming data to get the integrated record
  val clickresultDF = impressionDF.join(clickDF, impressionDF.col("impressionID") === clickDF.col("clickID"), "inner")
                       .drop(clickDF.col("clickID"))
      
  clickresultDF.printSchema()
  
  //Write the DF to the Sink with Update Mode as the last batch result is stored in Checkpoint
  //We need an update of the data stored in checkpoint using the current batch
  //Micro batch is created every 15 seconds
  val clickQuery = clickresultDF.writeStream
                  .format("console")
                  .outputMode("append")
                  .option("checkpointLocation", "checkPoint-join2")
                  .trigger(Trigger.ProcessingTime("15 second"))
                  .start()
  
  clickQuery.awaitTermination()
}