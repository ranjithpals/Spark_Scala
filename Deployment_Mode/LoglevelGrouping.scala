import org.apache.spark.SparkContext
import org.apache.log4j._

object LoglevelGrouping {
  
  //Define main method
  def main(args: Array[String]){
    
    //Set log Level
    Logger.getLogger("org").setLevel(Level.WARN)
    //Create a SparkContext
    val sc = new SparkContext()
    
    //Read Input file with the command line argument as the input file location
    val rdd1 = sc.textFile(args(0))
    
    val rdd2 = rdd1.map(x=>(x.split(":")(0), x.split(":")(1))).groupByKey.map(x=>(x._1, x._2.size))
    
    rdd2.collect().foreach(println)
    
  }
  
}