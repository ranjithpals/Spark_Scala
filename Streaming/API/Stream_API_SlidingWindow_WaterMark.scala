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

object Stream_API_TumblingWindow_WaterMark extends App {
  
  //Set the Log Level
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  //Create Spark Streaming Session
  val strSession = SparkSession.builder()
                   .appName("Streaming_TumblingWindow_Grouping_w_WaterMark")
                   .master("local[2]")
                   .config("spark.sql.shuffle.partitions", 3)
                   .config("spark.streaming.stopGracefullyOnShutdown", "true")
                   .getOrCreate()
                   
  //Read the Streaming data from Console
  val strdf = strSession.readStream
                .format("socket")
                .option("host", "g01.itversity.com")
                .option("port", 9992)
                .load()
                
  //Define Schema by constructing a StructType
  val orderSchema = StructType(List(
                StructField("order_id", IntegerType),
                StructField("order_date", TimestampType),
                StructField("order_customer_id", IntegerType),
                StructField("order_status", StringType),
                StructField("amount", FloatType)))
  
  //Extract the text added to Console - contained within default column "value" of the DataFrame
  // from_json method converts the text to JSON and places it under column name "schema_value"
  val valueDF = strdf.select(from_json(col("value"), orderSchema).alias("schema_value"))
  
  //valueDF.printSchema()
  
  //Selects all the tags within the JSON as individual columns under the DataFrame
  val refinedOrdersDF = valueDF.select("schema_value.*")
  
  //refinedOrdersDF.printSchema()
  
  //Transform the DF grouping it by order_date for data accumulated with the Window of x duration
  val groupedOrders = refinedOrdersDF
                      .withWatermark("order_date", "30 minute")
                      .groupBy(window(col("order_date"),"15 minute"))
                      .agg(sum(col("amount")).alias("total_amount"))
      
  //groupedOrders.printSchema()
                      
  val finalOrdersDF = groupedOrders.select("window.*", "total_amount")
 
  finalOrdersDF.printSchema()
  
  //Write the DF to the Sink with Update Mode as the last batch result is stored in Checkpoint
  //We need an update of the data stored in checkpoint using the current batch
  //Micro batch is created every 15 seconds
  val ordersQuery = finalOrdersDF.writeStream
                .format("console")
                .outputMode("update")
                .option("checkpointLocation", "checkPoint-Loc1")
                .trigger(Trigger.ProcessingTime("15 second"))
                .start()
  
  ordersQuery.awaitTermination()
}