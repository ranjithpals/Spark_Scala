import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object sparkcontext extends App{
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","sampleContext")
  sparkConf.set("spark.master","local[*]")
  
  val sc = new SparkContext(sparkConf)
  
}