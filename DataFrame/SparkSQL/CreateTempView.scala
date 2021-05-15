
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode


object CreateTempView extends App {
  
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
  
  //Create a Temp View of the DataFrame
  ordersDF.createOrReplaceTempView("orders")
  
  //Query the View to list of customers with highest orders with status 'CLOSED'
  val query = """
              select order_customer_id, count(*) as Closed_Orders from orders 
              where order_status = 'CLOSED'
              group by order_customer_id order by Closed_Orders desc
              limit 10
              """
  
  //Execute the Query to create a DataFrame
  val highestOrders = spSession.sql(query)
  
  //Display the DataFrame
  highestOrders.show()
  
  scala.io.StdIn.readLine()
  //Stop Session
  spSession.stop()
  
}