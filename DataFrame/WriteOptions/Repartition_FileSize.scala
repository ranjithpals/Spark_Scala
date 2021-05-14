import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode


object Repartition_FileSize extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  //Spark Configuration
  val conf = new SparkConf()
  .set("spark.app.name", "ch11_Project01")
  .set("spark.master", "local[*]")
  
  //Build Spark Session using the SparkConfiguration, if exists grab the existing session
  val spSession = SparkSession.builder()
  .config(conf)
  .getOrCreate()
  
  var filePath = "C:/Users/Owner/Documents/Trendy_Tech/Week-12/Datasets/orders.csv"
  
  //Read the CSV file
  val ordersDF = spSession.read
  .format("csv")
  .option("header", true)
  .option("inferSchema", true)
  .option("path", filePath)
  .load()
  
  //Find the number of Partitions
  val numPartitions = ordersDF.rdd.getNumPartitions
  
  println("Number of Partitions before repartition:"+ numPartitions)
  
  //Repartition to 8 files
  val ordersRP = ordersDF.repartition(4)
  
  println("Number of Partitions before repartition:"+ ordersRP.rdd.getNumPartitions)
  
  var opPath1 = "C:/Users/Owner/Documents/Trendy_Tech/Week-12/Output01/"
  
  //Write the DataFrame to CSV format with New Repartition
  ordersRP.write
  .format("csv")
  .mode(SaveMode.Overwrite)
  .option("path", opPath1)
  .save()
  
  var opPath2 = "C:/Users/Owner/Documents/Trendy_Tech/Week-12/Output02/"
  
  //Write the DataFrame to CSV format with maxRecordsperFile
  ordersDF.write
  .format("csv")
  .mode(SaveMode.Overwrite)
  .option("maxRecordsPerFile", 10000)
  .option("path", opPath2)
  .save()
  
  scala.io.StdIn.readLine()
  //Stop Session
  spSession.stop()
  
}