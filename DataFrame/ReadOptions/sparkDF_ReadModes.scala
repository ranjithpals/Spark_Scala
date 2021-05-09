
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import java.sql.Timestamp
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

object sparkDF_ReadModes extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val conf = new SparkConf()
  conf.set("spark.app.name", "readjson")
  conf.set("spark.master", "local[*]")
  
  val session = SparkSession.builder()
                .config(conf)
                .getOrCreate()
  
  val filePath = "C:/Users/Owner/Documents/Trendy_Tech/Week-11/Datasets/players.json"
  // Simple Function to read the JSON file as is
  //val ordersDF = session.read.json(filePath)
                
  //Read CSV File using READ options
  val playersDF: Dataset[Row] = session.read
  .format("json")
  .option("mode", "FAILFAST")
  .option("path", filePath)
  .load()
  
  //Print Schema of the DataFrame
  playersDF.printSchema
  
  import session.implicits._
  
  //Filter DataFrame
  //playersDF.filter("country == 'IND'").show()
  
  //Display DataFrame
  playersDF.show(10, false)
  
  scala.io.StdIn.readLine()
  //Stop Session
  session.stop()
}



