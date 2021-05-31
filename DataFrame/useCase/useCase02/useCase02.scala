
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalog.Catalog

object useCase02 extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val conf = new SparkConf()
  .set("spark.app.name", "useCase02")
  .set("spark.master", "local[*]")
  
  //Provide the configuration to the session options using the session builder 
  val session = SparkSession.builder()
  .config(conf)
  .enableHiveSupport()
  .getOrCreate()
  
  //Input File Path
  val filePath = "C:/Users/Owner/Documents/Trendy_Tech/Week-12/Datasets/biglog.txt"
  
  //Read CSV File using READ options
  val biglogDF = session.read
  .format("csv")
  .option("header", true)
  .option("inferSchema", "true")
  .option("path", filePath)
  .load()
  
  biglogDF.createTempView("biglog")
  
  // Display the count of Log Levels for each month ordered by Month represented by three characters
  val df1 = session.sql("""
            SELECT level, date_format(datetime, "MMM") as month, count(1)
            FROM biglog
            GROUP BY level, month
            ORDER BY level, month
    """)
    
  df1.show()
  
  // Display the count of Log Levels for each month which is ordered by Monthly number
  val df2 = session.sql("""
            SELECT level, date_format(datetime, "MMMM") as month, first(cast(date_format(datetime, "M") as Int)) as month_num, count(1) as cnt
            FROM biglog
            GROUP BY level, month
            ORDER BY level, month_num
    """)
    
  df2.select("level", "month", "cnt").show()
  
  //List of Months
  val months = List("January", "Febraury", "March", "April", "May", "June"
                    ,"July", "August", "September", "October", "November", "December")
  
  //Pivot table with 12 months as Columns and Log Levels are Rows
  val df3 = session.sql("""
            SELECT level, date_format(datetime, "MMMM") as month
            FROM biglog
            """).groupBy("level").pivot("month", months).count().show()
  
  
  //Capture the Log items of the Spark Session
  scala.io.StdIn.readLine()
  //Stop the Spark Session
  session.stop()
}