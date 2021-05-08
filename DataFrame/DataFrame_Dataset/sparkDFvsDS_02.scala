

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.sql.Timestamp
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

object sparkDFvsDS_02 extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val conf = new SparkConf()
  conf.set("spark.app.name", "readcsv")
  conf.set("spark.master", "local[*]")
  
  val session = SparkSession.builder()
                .config(conf)
                .getOrCreate()
  
  val filePath = "C:/Users/Owner/Documents/Trendy_Tech/Week-11/Datasets/orders.csv"
  // Function to read the CSV file as is
  //val ordersDF = session.read.csv(filePath)
                
  //Read CSV File using READ options
  val ordersDF: Dataset[Row] = session.read
  .option("header", true)
  .option("inferSchema", true)
  .csv(filePath)
  
  
  //Filter by Order_id, dataset of custom object type is type safe.
  ordersDF.filter("order_id > 1000")
  //Display the Dataset
  ordersDF.show(10)
  
  scala.io.StdIn.readLine()
  //Stop Session
  session.stop()
}



