import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object windowAggregations extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val conf = new SparkConf()
  .set("spark.app.name", "windowAggregations")
  .set("spark.master", "local[*]")
  
  //Instead of creating a configuration object, providing the session options within the session builder 
  val session = SparkSession.builder()
  .config(conf)
  .enableHiveSupport()
  .getOrCreate()
  
  //Input File Path
  val filePath = "C:/Users/Owner/Documents/Trendy_Tech/Week-12/Datasets/windowdata.csv"
  
  //Read CSV File using READ options
  val inputDF = session.read
  .format("csv")
  .option("header", false)
  .option("inferSchema", "true")
  .option("path", filePath)
  .load()
  
  import session.implicits._
  
  /* Window Function
   * Partition by Country
   * order by weeknum
   * Sum starting from First Record to the current row (First Record of the winodw)
   */

  // Add columns to the dataset
  val invoiceDF = inputDF.toDF("country", "weeknum", "numinvoices", "totalquantity", "invoicevalue")
  
  //Define the Window Function  
  val windowfunc = Window.partitionBy("country")
                          .orderBy("weeknum")
                          .rangeBetween(Window.unboundedPreceding, Window.currentRow)
  
  val finalDF = invoiceDF.withColumn("RunningTotal", sum("invoicevalue").over(windowfunc))
   
  // Display the DataFrame
  finalDF.show()
  
  //Capture the Log items of the Spark Session
  scala.io.StdIn.readLine()
  //Stop the Spark Session
  session.stop()
}