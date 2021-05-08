import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.sql.Timestamp
import org.apache.log4j.Level
import org.apache.log4j.Logger

//Class object for Dataset
case class ordersClass (order_id: Int, order_date: Timestamp, order_customer_id: Int, order_status: String)

object sparkDFvsDS_01 extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val conf = new SparkConf()
  conf.set("spark.app.name", "readcsv")
  conf.set("spark.master", "local[*]")
  
  val session = SparkSession.builder()
                .config(conf)
                .getOrCreate()
  
  val filePath = "C:/Users/Owner/Documents/Trendy_Tech/Week-11/Datasets/orders.csv"
  // Function to read the CSV file as is
  //val ordersDF = session.read.csv(filePath)
                
  //Read CSV File using READ options
  val ordersDF = session.read
  .option("header", true)
  .option("inferSchema", true)
  .csv(filePath)
  
  //Import encoder function, this has to be imported after creating the DataFrame
  import session.implicits._
  
  //Convert DataFrame to Dataset
  val ordersDs = ordersDF.as[ordersClass]
  //Filter by Order_id, dataset of custom object type is type safe.
  ordersDs.filter(_.order_id > 1000)
  //Display the Dataset
  ordersDs.show(5)
  
  scala.io.StdIn.readLine()
  //Stop Session
  session.stop()
}



