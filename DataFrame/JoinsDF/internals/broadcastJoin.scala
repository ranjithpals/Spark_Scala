

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalog.Catalog

object broadcastJoin extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val conf = new SparkConf()
  .set("spark.app.name", "internalsOfJoin")
  .set("spark.master", "local[*]")
  
  //Instead of creating a configuration object, providing the session options within the session builder 
  val session = SparkSession.builder()
  .config(conf)
  .enableHiveSupport()
  .getOrCreate()
  
  //Input File Path
  val filePath1 = "C:/Users/Owner/Documents/Trendy_Tech/Week-12/Orders/"
  
  //Read CSV File to load orders data
  val ordersDF = session.read
  .format("json")
  .option("header", true)
  .option("inferSchema", "true")
  .option("path", filePath1)
  .load()
  
  println("The number of Order Partitions: ", ordersDF.rdd.getNumPartitions)
  
  //Input File Path
  val filePath2 = "C:/Users/Owner/Documents/Trendy_Tech/Week-12/Customers"
  
  //Read CSV File using READ options
  val customersDF = session.read
  .format("json")
  .option("header", true)
  .option("inferSchema", "true")
  .option("path", filePath2)
  .load()
  
  //Set the autoBroadcast to False, so that the auto-broadcast does not occur automatically.
  session.sql("SET spark.sql.autoBroadcastJoinThreshold = -1")
  
  //Join the orders with customers using inner join to enable Auto Broadcasting
  val joinType2 = "inner"
  val joinCondition2 = ordersDF.col("order_customer_id") === customersDF.col("customer_id")
  val innerJoin = ordersDF.join(broadcast(customersDF), joinCondition2, joinType2).sort("order_id")
  innerJoin.show(50)
  
  //Capture the Log items of the Spark Session
  scala.io.StdIn.readLine()
  //Stop the Spark Session
  session.stop()
}