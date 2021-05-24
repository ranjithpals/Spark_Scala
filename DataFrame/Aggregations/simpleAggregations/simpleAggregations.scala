import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalog.Catalog

object simpleAggregations extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val conf = new SparkConf()
  .set("spark.app.name", "simpleAggregations")
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
  
  /*Find the following Aggregations
   * Number of Rows
   * Sum of Quantity
   * Avg Unit Price
   * Distinct Number of InvoiceNos
   */
  
  //Using Column Object
  ordersDF.select(
      count("*").as("RowCount"),
      sum("Quantity").as("TotalQuantity"),
      avg("UnitPrice").as("AvgUnitPrice"),
      countDistinct("InvoiceNo").as("DistinctInvoices")
  ).show()
  
  //Using SQL expressions
  ordersDF.selectExpr(
      "count(*) as RowCount",
      "sum(Quantity) as TotalQuantity",
      "avg(UnitPrice) as AvgUnitPrice",
      "count(distinct InvoiceNo) as DistinctInvoices"
  ).show()
  
  //Using SQL Queries
  ordersDF.createOrReplaceTempView("orders")
  
  val query = """
    select count(*) as RowCount, sum(Quantity) as TotalQuantity, 
    avg(UnitPrice) as AvgUnitPrice, count(distinct InvoiceNo) as DistinctInvoices
    from orders
    """
  session.sql(query).show()
      
  //Capture the Log items of the Spark Session
  scala.io.StdIn.readLine()
  //Stop the Spark Session
  session.stop()
}