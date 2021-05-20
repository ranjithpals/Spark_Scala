import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object trans_with_Regex extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val conf = new SparkConf()
  .set("spark.app.name", "trans_with_Regex")
  .set("spark.master", "local[*]")
  
  //Instead of creating a configuration object, providing the session options within the session builder 
  val session = SparkSession.builder()
  .config(conf)
  .enableHiveSupport()
  .getOrCreate()
  
  val filePath = "C:/Users/Owner/Documents/Trendy_Tech/Week-11/Datasets/orders_new.csv"
  
  //Read CSV File into RDD
  val ordersRDD = session.sparkContext.textFile(filePath)
  
  //Define a Regex to match each record in the Dataset
  val ordersRegex = """^(\S+) (\S+)\t(\S+)\,(\S+)""".r
  
  //Create a case class to convert each line to a custom Object 
  case class Orders(order_id:Int, order_customer_id:Int, order_status:String)
  
  //Function definition to convert record of RDD to Row of custom object type
  def lineParser(line: String) = {
    line match {
      case ordersRegex(order_id, order_date, order_customer_id ,order_status) =>
        Orders(order_id.toInt, order_customer_id.toInt, order_status)
    }
  }
  
  import session.implicits._
  
  //Convert RDD to Dataset 
  val ordersDS = ordersRDD.map(lineParser).toDS.cache()
  
  //Display the order id's of the dataset
  ordersDS.select("order_id").show()
  
  //Count of the each order status
  ordersDS.groupBy("order_status").count().show()
  
  scala.io.StdIn.readLine()
  //Stop Session
  session.stop()
}