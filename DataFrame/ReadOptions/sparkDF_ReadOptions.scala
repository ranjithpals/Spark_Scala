
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import java.sql.Timestamp
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

object sparkDFReadOptions extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val conf = new SparkConf()
  conf.set("spark.app.name", "readcsv")
  conf.set("spark.master", "local[*]")
  
  val session = SparkSession.builder()
                .config(conf)
                .getOrCreate()
  
  val filePath = "C:/Users/Owner/Documents/Trendy_Tech/Week-11/Datasets/users.parquet"
  
  //Read CSV File using READ options
  val ordersDF: Dataset[Row] = session.read
  .format("csv")
  .option("header", true)
  .option("inferSchema", true)
  .option("path", "C:/Users/Owner/Documents/Trendy_Tech/Week-11/Datasets/orders.csv")
  .load()
  
  //Filter by Order_id, dataset of custom object type is NOT type safe.
  ordersDF.filter("order_id > 1000")
  //Display the Dataset
  ordersDF.show(10, false)
  
  val usersDF = session.read
  .format("parquet") //**THIS IS OPTIONAL, DEFAULT OPTION IS PARQUET
  .option("path",filePath)
  .load()
  
  usersDF.show(10, false)
  
  scala.io.StdIn.readLine()
  //Stop Session
  session.stop()
}


