- **Writing a DataFrame to a Sink (Output)**
1. Create a RDD by reading from a file (Local or Cluster)
2. Convert RDD to RDD[Row]
3. Create a Schema using StructType
4. Convert RDD[Row] to DataFrame

- **Save Modes**
1. append - putting the file in the existing folder
2. overwrite - first delete the existing folder, and then create a new one
3. errorIfexists - will error out if output folder exists
4. ignore - if folder exists, it will ignore

- **File Formats**
1. Default File Format, if not specified is 'Parquet', DataFrame is saved in parquet format
2. Avro file format is not supported by default in version 2.4, but is available by default in 3.1 - current version.
3. For 2.4, we need to import an external JAR - https://mvnrepository.com/artifact/org.apache.spark/spark-avro_2.11/2.4.4
4. csv, json, parquet are built-in file formats

- **Find Number of Partitions**
1. Convert the DataFrame to RDD to get the Number of Partitions: DF.rdd.getNumPartitions

- **Spark File Layout**
1. **Repartition-Number of Files**
- Creates the exact number of files as mentoined in the repartition - df.repartition(num)
- Used to create more number of files which aids in achieving Parallelism for the downstream operations.
- But Partition Pruning is not possible as all the files have to be scanned for any filter operations.
2. **File size**
- Ensures each part file contains only n number of records
- Number of files is based on the total number of records within the dataframe/dataset
- .option("maxRecordsPerFile", 20000)
3. **Partitioning and Bucketing**
- Partition is similar to Partitioning in Hive: PartitionBy("col1", "col2",..)
- Bucketing is similar to Bucketing in Hive: BucketBy
4. **SortBy**
- 


Reference Links:

https://cloudxlab.com/assessment/displayslide/611/spark-sql-converting-rdd-to-dataframe-using-programmatic-schema

https://sparkbyexamples.com/apache-spark-rdd/convert-spark-rdd-to-dataframe-dataset/

https://stackoverflow.com/questions/39699107/spark-rdd-to-dataframe-python
