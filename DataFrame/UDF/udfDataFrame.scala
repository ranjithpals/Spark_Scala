import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalog.Catalog

object udfDataFrame extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val conf = new SparkConf()
  .set("spark.app.name", "udfDataFrame")
  .set("spark.master", "local[*]")
  
  //Instead of creating a configuration object, providing the session options within the session builder 
  val session = SparkSession.builder()
  .config(conf)
  .enableHiveSupport()
  .getOrCreate()
  
  //Input File Path
  val filePath = "C:/Users/Owner/Documents/Trendy_Tech/Week-12/Datasets/dataset1"
  
  case class Person(name: String, age: Int, city: String)
  
  //Read CSV File using READ options
  val peopleDF = session.read
  .format("csv")
  .option("header", true)
  .option("inferSchema", "true")
  .option("path", filePath)
  .load()
  
  def ageParser(age: Int): String = {
    if (age >= 18 ) "Y" else "N"
  }
  
  import session.implicits._
  
  val peopleDF1: Dataset[Row] = peopleDF.toDF("name", "age", "city")
  
  //adultBoolFunc is a Higher Order Function which takes a Function as a Input
  //Lower order API's like map (RDD) can use a normal function directly
  //Higher Order API's like withColumn (DF) need to use UDF which is registered with Driver
  val adultBoolFunc = udf(ageParser(_:Int): String)
  
  //Column Object expression UDF
  val peopleFinal01 = peopleDF1.withColumn("adult", adultBoolFunc(col("age")))
  
  //Sql/String expression UDF
  session.udf.register("adultBoolFunc1", ageParser(_:Int): String)
  
  val peopleFinal02 = peopleDF1.withColumn("adult", expr("adultBoolFunc1(age)"))
  
  //Anonymous Function
  session.udf.register("adultBoolFunc2", (x:Int) => {if (x >= 18) "Y" else "N"})
  
  val peopleFinal03 = peopleDF1.withColumn("adult", expr("adultBoolFunc2(age)"))
  
  peopleFinal03.show()
  
  //List all the functions registered with Spark
  session.catalog.listFunctions().filter(x => x.name == "adultBoolFunc1").show()
 
  peopleFinal03.createOrReplaceTempView("people")
  
  session.sql("select * from people").show()
  
  //Capture the Log items of the Spark Session
  scala.io.StdIn.readLine()
  //Stop the Spark Session
  session.stop()
}