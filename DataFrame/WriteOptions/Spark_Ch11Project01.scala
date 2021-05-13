import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.LongType


object Spark_Ch11Project01 extends App {
  
  //Set Logger level to ERROR. Capture only errors in the Log
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  //Input File Path
  val filePath = "C:/Users/Owner/Documents/Trendy_Tech/Week-11/Week11_assignment/windowdata.csv"
  
  val windowschema = StructType(List(
      StructField("country", StringType),
      StructField("weeknum", IntegerType),
      StructField("numinvoices", IntegerType),
      StructField("totalquantity", LongType),
      StructField("invoicevalue", DoubleType)
  ))
  
  //Spark Configuration
  val conf = new SparkConf()
  .set("spark.app.name", "ch11_Project01")
  .set("spark.master", "local[*]")
  
  //Build Spark Session using the SparkConfiguration, if exists grab the existing session
  val spSession = SparkSession.builder()
  .config(conf)
  .getOrCreate()
  
  //Load the file to the Spark Session
  val windowDF = spSession.read
  .format("csv")
  .schema(windowschema)
  .option("header", false)
  .option("path", filePath)
  .load
  
  val op_path = "C:/Users/Owner/Documents/Trendy_Tech/Week-11/Week11_assignment/Project1_op"
  
  //Write the DataFrame to Disk
  //Partition the DataFrame on country, weeknum
  windowDF.write
  .partitionBy("country", "weeknum")
  .parquet(op_path)
  
  println("File saved as Parquet")
  
  //Read the file which is saved to disk
  spSession.read
  .format("parquet")
  .option("path", op_path)
  .load().show(10)
  
  
  scala.io.StdIn.readLine()
  //Stop spark session
  spSession.stop()
  
}