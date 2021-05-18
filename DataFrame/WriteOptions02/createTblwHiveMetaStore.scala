import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

object createTblwHiveMetaStore extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  //Instead of creating a configuration object, providing the session options within the session builder 
  val session = SparkSession.builder()
  .appName("TblwHiveMetaStore")
  .master("local[*]")
  .enableHiveSupport()
  .getOrCreate()
  
  //Read the file
  val ordersDF = session.read
  .format("csv")
  .option("header", true)
  .option("inferSchema", true)
  .option("path", "C:/Users/Owner/Documents/Trendy_Tech/Week-11/Datasets/orders.csv")
  .load()
  
  //Create a Database to store the tables
  session.sql("create database if not exists retail")
  
  ordersDF.write
  .format("csv")
  .bucketBy(4, "order_id")
  .mode(SaveMode.Overwrite)
  .saveAsTable("retail.orders")
  //.option("path", value)
  
  //List all the tables listed in the catalog metastore
  session.catalog.listTables("retail").show()
  
  //Capture the Application flow within the Spark UI
  scala.io.StdIn.readLine()
  
  //Stop the session
  session.stop()
  
}