import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.column
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.expr

object select_ColumnsDF extends App {
    
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val conf = new SparkConf()
  .set("spark.app.name", "trans_with_Regex")
  .set("spark.master", "local[*]")
  
  //Instead of creating a configuration object, providing the session options within the session builder 
  val session = SparkSession.builder()
  .config(conf)
  .enableHiveSupport()
  .getOrCreate()
  
  val filePath = "C:/Users/Owner/Documents/Trendy_Tech/Week-11/Datasets/orders.csv"
  
  //Create a case class to convert each line to a custom Object 
  case class Orders(order_id:Int, order_customer_id:Int, order_status:String)
  
  //Read CSV File using READ options
  val ordersDF = session.read
  .format("csv")
  .option("header", true)
  .option("inferSchema", true)
  .option("path", filePath)
  .load()
  
  import session.implicits._
  
  //Select columns -> Column String
  ordersDF.select("order_id", "order_status").show()
  
  //Select Columns -> Column Object: Scala Specific syntactic sugar
  ordersDF.select($"order_id", 'order_status).show() 
  
  //Select Columns -> Column Object: column("<name>") or col("<name>")
  ordersDF.select(column("order_id"), col("order_customer_id")).show()
  
  //Select Columns -> Column Expressions
  //ordersDF.select("concat(order_status, '_STATUS')").show()
  
  //Select Columns -> Convert Column Expressions to Column Object
  ordersDF.select(column("order_id"), expr("concat(order_status, '_STATUS')")).show()
  
  //Select Columns -> Use .selectExpr to use all col strings along with expressions without explicit conversions
  ordersDF.selectExpr("order_id", "order_date", "concat(order_status, '_STATUS')").show(false)
  
  //Select Columns -> Column Expression
  //ordersDF.selectExpr(")
  
  scala.io.StdIn.readLine()
  //Stop Session
  session.stop()
}