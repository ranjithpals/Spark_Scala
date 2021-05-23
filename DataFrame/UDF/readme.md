 **Spark UDF's**
 > Lower order API's like map (RDD) can use a normal function directly
 > Higher Order API's like withColumn (DF) need to use UDF which is registered with Driver
 **Different ways of Creating UDF's and Regsiter the UDF's with Spark**
 ```
   def ageParser(age: Int): String = {
    if (age >= 18 ) "Y" else "N"
  }
 ```
 **Column Object expression UDF**
 - Create UDF by creating a Higher Order Function as it takes a Function as Input **Not registered with Spark Driver**
 ```
 val adultBoolFunc = udf(ageParser(_:Int): String)
 val peopleFinal01 = peopleDF1.withColumn("adult", adultBoolFunc(col("age")))
 ```
 **SQL/String expression UDF**
 - Create UDF and register with Spark Driver
 ```
 session.udf.register("adultBoolFunc1", ageParser(_:Int): String)
 val peopleFinal02 = peopleDF1.withColumn("adult", expr("adultBoolFunc1(age)"))
 ```
 **Using Anonymous Function**
 ```
 session.udf.register("adultBoolFunc2", (x:Int) => {if (x >= 18) "Y" else "N"})
 val peopleFinal03 = peopleDF1.withColumn("adult", expr("adultBoolFunc2(age)"))
 ```
 **List the functions using Catalog library**
 ```
 session.catalog.listFunctions().filter(x => x.name == "adultBoolFunc1").show()
 ```
