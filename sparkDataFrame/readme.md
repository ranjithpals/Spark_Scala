Read CSV script has 3 Actions (Jobs)

![DF1.png](3 Actions for this sparkReadCsv.scala)

1. Job 0 - To read the file, Spark creates internal partitions to construct the underlying RDD, this creates an action
2. Job 1 - To infer the schema, the data (sample of records) needs to be serialized, this creates an action
3. Job 2 - To display the data, this creates an action
