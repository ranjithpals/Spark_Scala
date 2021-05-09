**Calling the Read Function**
```
  //Read CSV File using READ options
  val ordersDF: Dataset[Row] = session.read
  .format("csv")
  .option("header", true)
  .option("inferSchema", true)
  .option("path", filePath)
  .load()
```
**JSON FILE**
```
  //Read CSV File using READ options
  val playersDF: Dataset[Row] = session.read
  .format("json")
  .option("mode", "FAILFAST")
  .option("path", filePath)
  .load()
```
**PARQUET FILE**
```
  val usersDF = session.read
  .format("parquet") //**THIS IS OPTIONAL, DEFAULT OPTION IS PARQUET
  .option("path",filePath)
  .load()
  
  usersDF.show(10, false)
```
**READ MODES**
1. **PERMISSIVE** - Default Mode 
- It sets all fields to NULL when encounters a corrupted record. 
- It also creates a new column (_corrupted_record_) and displays the corrupted record.
```
+-----------------------------------------------------------------------------------------------------+----+-------+---------+-----------+----------+-------+
|_corrupt_record                                                                                      |age |country|player_id|player_name|role      |team_id|
+-----------------------------------------------------------------------------------------------------+----+-------+---------+-----------+----------+-------+
|null                                                                                                 |33  |IND    |101      |R Sharma   |Batsman   |11     |
|null                                                                                                 |25  |IND    |102      |S Iyer     |Batsman   |15     |
|null                                                                                                 |30  |NZ     |103      |T Boult    |Bowler    |13     |
|null                                                                                                 |38  |IND    |104      |MS Dhoni   |WKeeper   |14     |
|null                                                                                                 |39  |AUS    |105      |S Watson   |Allrounder|12     |
|{"player_id":106, "player_name":"S Hetmyer", "age":23, "role":"Batsman", "team_id":16, "country":"WI"|null|null   |null     |null       |null      |null   |
|}                                                                                                    |null|null   |null     |null       |null      |null   |
+-----------------------------------------------------------------------------------------------------+----+-------+---------+-----------+----------+-------+
```
2. **DROPMALFORMED** - The corrupted record is ignored
3. **FAILFAST** - An exception is raised whenever it encounters a corrupt record.
```
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
21/05/09 06:34:14 ERROR Executor: Exception in task 1.0 in stage 0.0 (TID 1)
org.apache.spark.SparkException: Malformed records are detected in schema inference. Parse Mode: FAILFAST.
at org.apache.spark.sql.catalyst.json.JsonInferSchema$$anonfun$1$$anonfun$apply$1.apply(JsonInferSchema.scala:66)
```

**SCHEMA OPTIONS**
1. INFER - inferSchema OPTION
2. IMPLICIT - PARQUET, AVRO format etc.
3. EXPLICIT - Manually defining the SCHEMA [StructType, DDL string, case class (<definition>)]

**SPARK DATAFRAME OPERATIONS**
1. Read the data from DataSource and create a DataFrame/Datasets.
  - External Data Source: Data is NOT stored in distributed environment (mySQL database, redshift, mongodb)
  - Internal Data Source: Data is stored in distributed environment (hdfs, S3, azure blob, google storage)
2. Spark is generally good at processing but is not efficient at ingesting data.
3. Before Spark processes data, ingest data into an Internal data source before applying Transformations and Actions.
4. Spark gives you a JDBC connector to ingest the data from mysql database directly.
5. **Recommended** - Use tools like Sqoop which are specifically made for data ingestion to get your data from external source to internal source
6. Write the data to the Target (SINK)
