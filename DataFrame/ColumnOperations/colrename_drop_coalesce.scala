

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalog.Catalog

object colrename_drop_coalesce extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val conf = new SparkConf()
  .set("spark.app.name", "colrename_drop_coalesce")
  .set("spark.master", "local[*]")
  
  //Instead of creating a configuration object, providing the session options within the session builder 
  val session = SparkSession.builder()
  .config(conf)
  .enableHiveSupport()
  .getOrCreate()
  
  //Input File Path
  val filePath1 = "C:/Users/Owner/Documents/Trendy_Tech/Week-12/Datasets/orders.csv"
  
  //Read CSV File to load orders data
  val ordersDFwh = session.read
  .format("csv")
  .option("header", true)
  .option("inferSchema", "true")
  .option("path", filePath1)
  .load()
  
  //Extract only the first row (header) of the DF
  val header = ordersDFwh.first()
  
  //Remove the header from the DF
  val ordersDF = ordersDFwh.filter(line => line != header).toDF("order_id",	"order_date",	"customer_id",	"order_status")
  
  //Input File Path
  val filePath2 = "C:/Users/Owner/Documents/Trendy_Tech/Week-12/Datasets/customers.csv"
  
  //Read CSV File using READ options
  val customersDF = session.read
  .format("csv")
  .option("header", true)
  .option("inferSchema", "true")
  .option("path", filePath2)
  .load()
  
  //Since both the DF's have customer_id, the final dataset would have a ambigious column, hence need to rename one.
  val ordersNew = ordersDF.withColumnRenamed("customer_id", "cust_id")
  
  //Join the orders with customers using inner join to show only customers who have placed an order
  val joinType1 = "outer"
  val joinCondition1 = ordersNew.col("cust_id") === customersDF.col("customer_id")
  val outerJoin1 = ordersNew.join(customersDF, joinCondition1, joinType1)
  outerJoin1.show(5)
  
  //Other Option is to Drop one of the customer_id columns after performing the join operation
  val joinType2 = "right"
  val joinCondition2 = ordersDF.col("customer_id") === customersDF.col("customer_id")
  val outerJoin2 = ordersDF.join(customersDF, joinCondition2, joinType2).drop(ordersDF.col("customer_id"))
  outerJoin2.show(5)
  
  //Replace Null values with a common representation for such values
  val outerjoin3 = outerJoin2
  .select("order_id", "customer_fname", "customer_id", "order_status")
  .sort("order_id")
  .withColumn("order_id", expr("coalesce(order_id, -1)"))
  .show(100)
  
  //Capture the Log items of the Spark Session
  scala.io.StdIn.readLine()
  //Stop the Spark Session
  session.stop()
}