
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object sparksession extends App {
  
  val conf = new SparkConf()
  conf.set("spark.app.name", "SampleApp")
  conf.set("spark.master", "local[*]")
  
  val spark = SparkSession.builder()
  .config(conf)
  .enableHiveSupport()
  .getOrCreate()
  
  // Two-ways to get access to SparkContext from SparkSession
  //val spark_context = spark._sc
  val spark_context01 = spark.sparkContext
  
  /*
  val spark = SparkSession.builder()
  .appName("SampleApp")
  .master("master")
  .getOrCreate()
  */
  
  println(spark.sql("select 1"))
  scala.io.StdIn.readLine()
  //Stop the Session
  spark.stop()
  

}