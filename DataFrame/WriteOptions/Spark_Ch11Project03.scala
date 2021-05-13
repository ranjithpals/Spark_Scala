import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.LongType
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object Spark_Ch11Project03 extends App {
  
  //Set Logger level to ERROR. Capture only errors in the Log
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  //Input File Path
  val filePath = "C:/Users/Owner/Documents/Trendy_Tech/Week-11/Week11_assignment/windowdata.csv"
  
  val windowschema = StructType(List(
      StructField("country", StringType),
      StructField("weeknum", IntegerType),
      StructField("numinvoices", IntegerType),
      StructField("totalquantity", LongType),
      StructField("invoicevalue", DoubleType)
  ))
  
  //Spark Configuration
  val conf = new SparkConf()
  .set("spark.app.name", "ch11_Project01")
  .set("spark.master", "local[*]")
  
  //Build Spark Session using the SparkConfiguration, if exists grab the existing session
  val session = SparkSession.builder()
  .config(conf)
  .getOrCreate()
  
  /*Create a SparkContext, two ways of creating it
  1. Create a new SparkContext
  2. Retrieve the SparkContext from the existing SparkSession
  */
  
  //val spContext = new SparkContext(conf)
  val spContext = session.sparkContext

  //Create a RDD by loading file to the Spark Session
  val RDD = spContext.textFile(filePath)
  
  //Convert RDD to RDD[Row]
  val rowRDD = RDD.map(x=>x.split(",")).map(x=>Row(x(0),x(1).toInt, x(2).toInt, x(3).toLong, x(4).toDouble))
  
  import session.implicits._
  
  //Convert a RDD[Row] to DF using the toDF method.
  //use a list of strings defining the columns
  val winDF = RDD.toDF() //"country weeknum numinvoices totalquantity invoicevalue"
  
  println("Display the DataFrame created from RDD using the toDF() function")
  winDF.show(10,truncate=false)
  
  //Convert RDD[Row] to DataFrame with a Schema defined using StructType
  val windowDF = session.createDataFrame(rowRDD, windowschema)
   
  println("Display the DataFrame created from RDD[Row] created using the .createDataFrame() function")
  windowDF.show(10)
  
  val op_path = "C:/Users/Owner/Documents/Trendy_Tech/Week-11/Week11_assignment/Project2_op1"
  
  //Save as JSON file
  windowDF.coalesce(8).write
  .format("json")
  .option("path", op_path)
  .save()
  
  scala.io.StdIn.readLine()
  //Stop spark session
  session.stop()
  
}