import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalog.Catalog

object Salting_wStaticResources extends App{
  
  // Not required if running from spark-shell
  val session = SparkSession.builder().getOrCreate()
  val sc = session.sparkContext
  
  //Run the below code on spark-shell (Till collect operation)
  //spark2-shell --conf spark.dynamincAllocation.enabled=false --master yarn --num-executors 5 --executor-cores 2 --executor-memory 2G
  val random = new scala.util.Random
  val start = 1
  val end = 60
  
  // Read the file
  val rdd1 = sc.textFile("bigLogFinal.txt")
  // Perform Salting to, have more number of unique key values 
  val rdd2 = rdd1.map(x=> {var num = start + random.nextInt((end-start)+1)
  (x.split(':')(0)+num, x.split(':')(1))})
  // Group By key
  val rdd3 = rdd2.groupByKey()
  // Count the Key values
  val rdd4 = rdd3.map(x=>(x._1, x._2.size))
  // Normalize the WARN and ERROR keys
  val rdd5 = rdd4.map(x=> {if (x._1.substring(0,4) == "WARN") ("WARN", x._2) else ("ERROR", x._2)})
  // Reduce By Key
  val rdd6 = rdd5.reduceByKey(_+_)
  // Action
  rdd6.collect().foreach(println)
}