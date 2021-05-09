import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger


object sparkDF_SchemaDef_DDL extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val conf = new SparkConf()
  conf.set("spark.app.name", "readcsv")
  conf.set("spark.master", "local[*]")
  
  val session = SparkSession.builder()
                .config(conf)
                .getOrCreate()
                
  val ordersSchemaDDL = "orderid Int, orderdate String, ordercustomerid Int, orderstatus String"
  
  val filePath = "C:/Users/Owner/Documents/Trendy_Tech/Week-11/Datasets/orders.csv"
  
  //Read CSV File using READ options
  val ordersDF = session.read
  .format("csv")
  .option("header", true)
  .schema(ordersSchemaDDL)
  .option("path", filePath)
  .load()
  
  //Filter by Order_id, dataset of custom object type is NOT type safe.
  ordersDF.filter("orderid > 1000").show(10, false)
  
  //Display the Dataset
  //ordersDF.show(10, false)
  
  scala.io.StdIn.readLine()
  //Stop Session
  session.stop()
}


