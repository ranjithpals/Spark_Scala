import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext


object sqlcontext extends App {
  
  val conf = new SparkConf()
  conf.set("spark.app.name", "sqlcontext")
  conf.set("spark.master", "local[*]")
  
  // create sparkcontext
  val sc = new SparkContext(conf)
  // create sqlcontext from a sparkcontext object
  val sqlcontext = new SQLContext(sc)
  
  // Stop sparkcontext
  sc.stop()
}