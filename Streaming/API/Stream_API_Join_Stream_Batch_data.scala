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

object Stream_API_Join_Stream_Batch_data extends App {
  
  //Set the Log Level
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  //Create Spark Streaming Session
  val strSession = SparkSession.builder()
                   .appName("Stream_API_Join_Stream_Batch_data")
                   .master("local[2]")
                   .config("spark.sql.shuffle.partitions", 3)
                   .config("spark.streaming.stopGracefullyOnShutdown", "true")
                   .getOrCreate()
                   
  //Read the Streaming data on POS using a card from Console
  val strdf = strSession.readStream
                .format("socket")
                .option("host", "g01.itversity.com")
                .option("port", 12345)
                .load()
                
  //Define Schema by constructing a StructType
  val transactionSchema = StructType(List(
                          StructField("card_id", LongType),
                          StructField("amount", FloatType),
                          StructField("post_code", IntegerType),
                          StructField("pos_id", LongType),
                          StructField("transaction_dt", TimestampType)))
  
  //Extract the text added to Console - contained within default column "value" of the DataFrame
  // from_json method converts the text to JSON of defined Schema 
  // and places it under column name "json_val"
  val valueDF = strdf.select(from_json(col("value"), transactionSchema).alias("json_val"))
  
  //valueDF.printSchema()
  
  //Selects all the tags within the JSON as individual columns under the DataFrame
  val refinedOrdersDF = valueDF.select("json_val.*")
  
  //refinedOrdersDF.printSchema()
  
  //Read the Batch data with card account related information
  val batchDF = strSession.read
                .format("csv")
                .option("path", "C:/Users/Owner/Documents/Trendy_Tech/Week-16/Datasets/card_details.csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load()
                
  //Join the Streaming and Batch data to get the integrated record
  val joinCardDF = refinedOrdersDF.join(batchDF, refinedOrdersDF.col("card_id") === batchDF.col("card_id"), "inner")
                   .drop(batchDF.col("card_id"))
      
  joinCardDF.printSchema()
  
  //Write the DF to the Sink with Update Mode as the last batch result is stored in Checkpoint
  //We need an update of the data stored in checkpoint using the current batch
  //Micro batch is created every 15 seconds
  val cardQuery = joinCardDF.writeStream
                  .format("console")
                  .outputMode("update")
                  .option("checkpointLocation", "checkPoint-join1")
                  .trigger(Trigger.ProcessingTime("15 second"))
                  .start()
  
  cardQuery.awaitTermination()
}