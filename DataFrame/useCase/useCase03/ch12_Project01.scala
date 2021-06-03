import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ch12_Project01 extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  //Spark Configuration object
  val conf = new SparkConf()
  .set("spark.app.name", "Project01")
  .set("spark.master", "local[*]")
  
  //Spark session
  val session = SparkSession.builder()
  .config(conf)
  .getOrCreate()
  
    
  import session.implicits._
  
  //Load the Employee dataset
  val emp = session.read
  .format("json")
  .option("path", "C:/Users/Owner/Documents/Trendy_Tech/Week-12/Assignment/employee.json")
  .option("inferSchema", "true")
  .load()
  
  //emp.show(false)
  
  //Load the Department dataset
  val dept = session.read
  .format("json")
  .option("path", "C:/Users/Owner/Documents/Trendy_Tech/Week-12/Assignment/dept.json")
  .option("inferSchema", "true")
  .load()

  val new_emp = emp.withColumnRenamed("deptid", "emp_deptid")
  //dept.show()
  
  //Join both the datasets
  val jointype = "left"
  val joinCondition = new_emp.col("emp_deptid") === dept.col("deptid")
  
  val joinDF = dept.join(new_emp, joinCondition, jointype).select("deptid", "deptName", "id")
  val finalDF = joinDF.groupBy("deptid", "deptName").agg(count("id").as("empcount")).sort("deptid")
  
  //Show final DF
  finalDF.show()
}