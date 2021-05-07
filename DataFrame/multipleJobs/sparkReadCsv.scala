import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object spReadCSV extends App{
  
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
  
  ordersDF.show()
  
  ordersDF.printSchema()
  
  scala.io.StdIn.readLine()
  //Stop Session
  session.stop()
}