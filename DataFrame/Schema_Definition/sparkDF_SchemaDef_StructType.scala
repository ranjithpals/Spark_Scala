import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import java.sql.Timestamp
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DateType


object sparkDF_SchemaDefinition extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val conf = new SparkConf()
  conf.set("spark.app.name", "readcsv")
  conf.set("spark.master", "local[*]")
  
  val session = SparkSession.builder()
                .config(conf)
                .getOrCreate()
                
  val Orders_Struct = StructType(List(
      StructField("orderid", IntegerType, false), //false -> Nullable is FALSE
      StructField("orderdate", DateType),
      StructField("ordercustomerid", IntegerType),
      StructField("orderstatus", StringType)
  ))
  
  val filePath = "C:/Users/Owner/Documents/Trendy_Tech/Week-11/Datasets/orders.csv"
  
  //Read CSV File using READ options
  val ordersDF = session.read
  .format("csv")
  .option("header", true)
  .schema(Orders_Struct)
  .option("path", filePath)
  .load()
  
  //Filter by Order_id, dataset of custom object type is NOT type safe.
  ordersDF.filter("orderid > 1000").show(10, false)
  
  //Display the Dataset
  //ordersDF.show(10, false)
  
  scala.io.StdIn.readLine()
  //Stop Session
  session.stop()
}


