

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._

object groupingAggregate extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val conf = new SparkConf()
  .set("spark.app.name", "groupingAggregate")
  .set("spark.master", "local[*]")
  
  //Instead of creating a configuration object, providing the session options within the session builder 
  val session = SparkSession.builder()
  .config(conf)
  .enableHiveSupport()
  .getOrCreate()
  
  //Input File Path
  val filePath = "C:/Users/Owner/Documents/Trendy_Tech/Week-12/Datasets/order_data.csv"
  
  //Read CSV File using READ options
  val ordersDF = session.read
  .format("csv")
  .option("header", true)
  .option("inferSchema", "true")
  .option("path", filePath)
  .load()
  
  import session.implicits._
  /*
   * Group the dataset on Country, InvoiceNo
   * Total Quantity for each group
   * Sum of each Invoice's Value
   */
  
  //Using Column Object
  val groupedDF1 = ordersDF.groupBy("Country", "InvoiceNo")
                   .agg(sum("Quantity").as("TotalQuantity"),
                    sum(expr("Quantity * UnitPrice")).as("SumofInvoiceValue"))
  groupedDF1.show()
  
  //Using Expressions
  val groupedDF2 = ordersDF.groupBy("Country", "InvoiceNo")
                    .agg(expr("sum(Quantity) as TotalQuantity"),
                    expr("sum(Quantity*UnitPrice) as SumofInvoiceValue"))
  groupedDF2.show()
  
  //Using SQL queries
  ordersDF.createOrReplaceTempView("orders")
  
  val query = """
              SELECT Country, InvoiceNo, sum(Quantity) as TotalQuantity, sum(Quantity*UnitPrice) as SumofInvoiceValue
              FROM orders
              GROUP BY Country, InvoiceNo 
              """
  
  session.sql(query).show()
  
  //Capture the Log items of the Spark Session
  scala.io.StdIn.readLine()
  //Stop the Spark Session
  session.stop()
}