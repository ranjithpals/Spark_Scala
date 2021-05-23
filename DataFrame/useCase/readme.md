**Use Case 01**
- Convert Scala List to DataFrame - df.createDataFrame(list)
- Convert Date (string data type) to Epoch time (unixtimestamp) - number of seconds after 01-01-1970
> .withColumn("orderdate", unix_timestamp(col("orderdate").cast(DateType)))
- create new column with unique id's
> .withColumn("newid", monotonically_increasing_id)
- Drop Duplicates based on orderdate and customerid
> .dropDuplicates("orderdate", "customerid")
- Drop Columns
> .drop("orderid")
- Sortby Columns
> .sort("orderdate")