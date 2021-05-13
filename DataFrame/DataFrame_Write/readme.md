**CREATE DATAFRAME FROM RDD**
- Using createDataFrame method and Schema
1. Create a RDD by reading from a file (Local or Cluster)
2. Convert RDD to RDD[Row]
3. Create a Schema using StructType
4. Convert RDD[Row] to DataFrame

- Avro file format is not supported by default in version 2.4, but is available by default in 3.1 - current version. 
- For 2.4, we need to import an external JAR
- https://mvnrepository.com/artifact/org.apache.spark/spark-avro_2.11/2.4.4

Reference Links:

https://cloudxlab.com/assessment/displayslide/611/spark-sql-converting-rdd-to-dataframe-using-programmatic-schema

https://sparkbyexamples.com/apache-spark-rdd/convert-spark-rdd-to-dataframe-dataset/

https://stackoverflow.com/questions/39699107/spark-rdd-to-dataframe-python
