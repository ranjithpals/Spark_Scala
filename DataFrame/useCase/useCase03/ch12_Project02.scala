import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ch12_Project02 extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  //Spark Configuration object
  val conf = new SparkConf()
  .set("spark.app.name", "Project02")
  .set("spark.master", "local[*]")
  
  //Spark session
  val session = SparkSession.builder()
  .config(conf)
  .getOrCreate()
  
  //Spark Context
  val sc = session.sparkContext
  
  import session.implicits._
  
  //Load the Movie dataset
  val movie1 = sc.textFile("C:/Users/Owner/Documents/Trendy_Tech/Week-11/Datasets/movies.dat")
  val movie2 = movie1.map(_.split("::")).map(x=> (x(0), x(1), x(2))).toDF("Sno", "Name", "Genre")
  
  //movie2.show()
  
  //Load the Ratings dataset
  val rating1 = sc.textFile("C:/Users/Owner/Documents/Trendy_Tech/Week-11/Datasets/ratings.dat")
  val rating2 = rating1.map(_.split("::")).map(x=> (x(0), x(2))).toDF("MovieNo", "Rating")
  
  //Option-1
  //Calculate Average of Rating
  //val rating3 = rating2.groupBy("MovieNo").agg(avg("Rating").as("Rating"))
  //Round of Avg Rating
  //val rating = rating3.withColumn("Rating", expr("round(Rating, 1)"))
  
  //rating.show
  
  //Join both the datasets
  session.sql("SET spark.sql.autoBroadcastJoinThreshold = -1")
  
  val jointype = "right"
  val joinCondition = rating2.col("MovieNo") === movie2.col("Sno")
  
  //Option-1
  //val finalDF = rating2.join(broadcast(movie2), joinCondition, jointype).select("Name", "Rating").sort(col("Rating").desc)
  
  val joinDF = rating2.join(broadcast(movie2), joinCondition, jointype).groupBy("MovieNo", "Name").agg(avg("Rating").as("Rating"))
  val finalDF = joinDF.withColumn("Rating", expr("round(Rating, 1)")).select("Name", "Rating").sort(col("Rating").desc)
  
  //Show final DF
  finalDF.show(50, false)
  
  scala.io.StdIn.readLine()
  //Stop Session
  session.stop()

}