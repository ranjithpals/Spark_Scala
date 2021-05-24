import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType

object sampleUseCase01 extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val conf = new SparkConf()
  .set("spark.app.name", "sampleUseCase01")
  .set("spark.master", "local[*]")
  
  //Instead of creating a configuration object, providing the session options within the session builder 
  val session = SparkSession.builder()
  .config(conf)
  .enableHiveSupport()
  .getOrCreate()
  
  //Input File Path
  val Input = List(("2013-07-25", 11599, "CLOSED"),
                   ("2013-07-25", 11599, "COMPLETE"),
                   ("2014-07-25", 256, "PENDING_PAYMENT"),
                   ("2019-07-25", 8827, "CLOSED"))
  
  import session.implicits._
  
  //Create DF from List and assign columns                
  val inputDF = session.createDataFrame(Input)
  .toDF("orderdate", "customerid", "orderstatus")
  //Convert the date column to Epoch Date format
  val inputDF1 = inputDF
  .withColumn("orderdate", unix_timestamp(col("orderdate").cast(DateType)))
  .withColumn("newid", monotonically_increasing_id)
  .dropDuplicates("orderdate", "customerid")
  .drop("orderid")
  .sort("orderdate")
  .show()
  
  //Capture the Log items of the Spark Session
  scala.io.StdIn.readLine()
  //Stop the Spark Session
  session.stop()
}