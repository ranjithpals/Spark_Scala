import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object sparkTransformations extends App {
  
  val conf = new SparkConf()
  conf.set("spark.app.name", "transfomrations")
  conf.set("spark.master", "local[*]")
  
  val session = SparkSession.builder()
  .config(conf)
  .enableHiveSupport()
  .getOrCreate()
  
  val ordersDF = session.read
  .option("inferSchema", true)
  .option("header", true)
  .csv("C:/Users/Owner/Documents/Trendy_Tech/Week-11/Datasets/orders.csv")
  
  val groupedOrdersDF = ordersDF.repartition(4)
  .filter("order_customer_id > 1000")
  .select("order_id", "order_customer_id")
  .groupBy("order_customer_id")
  .count()
  
  //Display Grouped DF
  groupedOrdersDF.show()
  
  //Display Schema
  groupedOrdersDF.printSchema()
  
  scala.io.StdIn.readLine()
  session.stop()
  
}