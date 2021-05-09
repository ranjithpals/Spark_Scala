**SCHEMA OPTIONS**

- INFER - inferSchema OPTION
- IMPLICIT - PARQUET, AVRO format etc.
- EXPLICIT - Manually defining the SCHEMA (StructType, DDL string, case class)

**EXPLICIT SCHEMA**
- StructType
- DDL String

1. **StructType**
```
  val Orders_Struct = StructType(List(
      StructField("orderid", IntegerType, false), //false -> Nullable is FALSE
      StructField("orderdate", DateType),
      StructField("ordercustomerid", IntegerType),
      StructField("orderstatus", StringType)
  ))
  
  val filePath = "C:/Users/Owner/Documents/Trendy_Tech/Week-11/Datasets/orders.csv"
  
  //Read CSV File using READ options
  val ordersDF = session.read
  .format("csv")
  .option("header", true)
  .schema(Orders_Struct)
  .option("path", filePath)
  .load()
  
  //Filter by Order_id, dataset of custom object type is NOT type safe.
  ordersDF.filter("orderid > 1000").show(10, false)
  ```
  
 2. **DDL String**
 ```
 val ordersSchemaDDL = "orderid Int, orderdate String, ordercustomerid Int, orderstatus String"
 
  //Read CSV File using READ options
  val ordersDF = session.read
  .format("csv")
  .option("header", true)
  .schema(ordersSchemaDDL)
  .option("path", filePath)
  .load()
 ```
 - The Scala Data Type needs type casting, which is less efficient, reading the data **using Spark DataType is MORE EFFICIENT**
 > Scala      | Spark
 1. Int       | Integer
 2. Long      | LongType
 3. Float     | FloatType
 4. Double    | DoubleType

3. **Convert DataFrame to Dataset**
- Casting to Custom Object defined using case class (<definition>)
- Dataset [object type] provides strongly typed code
- filter function uses the column name instead of String condition, which identifies errors at Compile Time.

```
//Class object for Dataset
case class ordersClass (order_id: Int, order_date: Timestamp, order_customer_id: Int, order_status: String)

  //Import encoder function, this has to be imported after creating the DataFrame
  import session.implicits._
  
  //Convert DataFrame to Dataset
  val ordersDs = ordersDF.as[ordersClass]
  //Filter by Order_id, dataset of custom object type is type safe.
  ordersDs.filter(_.order_id > 1000) // same as ordersDF.filter(x => x.order_id > 1000)
  //Display the Dataset
  ordersDs.show(5)
  ```
