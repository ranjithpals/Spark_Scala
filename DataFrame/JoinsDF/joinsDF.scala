
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalog.Catalog

object joinsDF extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val conf = new SparkConf()
  .set("spark.app.name", "joinsDF")
  .set("spark.master", "local[*]")
  
  //Instead of creating a configuration object, providing the session options within the session builder 
  val session = SparkSession.builder()
  .config(conf)
  .enableHiveSupport()
  .getOrCreate()
  
  //Input File Path
  val filePath1 = "C:/Users/Owner/Documents/Trendy_Tech/Week-12/Datasets/orders.csv"
  
  //Read CSV File to load orders data
  val ordersDF = session.read
  .format("csv")
  .option("header", true)
  .option("inferSchema", "true")
  .option("path", filePath1)
  .load()
  
  //Input File Path
  val filePath2 = "C:/Users/Owner/Documents/Trendy_Tech/Week-12/Datasets/customers.csv"
  
  //Read CSV File using READ options
  val customersDF = session.read
  .format("csv")
  .option("header", true)
  .option("inferSchema", "true")
  .option("path", filePath2)
  .load()
  
  //Join the orders with customers using inner join to show only customers who have placed an order
  val joinType1 = "inner"
  val joinCondition1 = ordersDF.col("order_customer_id") === customersDF.col("customer_id")
  val innerJoin = ordersDF.join(customersDF, joinCondition1, joinType1)
  innerJoin.show()
  
  //Join the orders with customers using Right join to show all customers irrespective of placing an order or NOT and order by order_id
  val joinType2 = "right"
  val joinCondition2 = ordersDF.col("order_customer_id") === customersDF.col("customer_id")
  val rightJoin = ordersDF.join(customersDF, joinCondition2, joinType2).orderBy("order_id")
  rightJoin.show()
  
  //Capture the Log items of the Spark Session
  scala.io.StdIn.readLine()
  //Stop the Spark Session
  session.stop()
}