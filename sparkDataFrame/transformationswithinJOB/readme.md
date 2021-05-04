Spark count() function is a Action? or Tranformation?

https://stackoverflow.com/questions/52966347/spark-is-count-on-grouped-data-a-transformation-or-an-action

The .count() what you have used in your code is over RelationalGroupedDataset, which creates a new column with count of elements in the grouped dataset. This is a transformation.
Refer: https://spark.apache.org/docs/1.6.0/api/scala/index.html#org.apache.spark.sql.GroupedDataset

The .count() that you use normally over RDD/DataFrame/Dataset is completely different from the above and this .count() is an Action.
Refer: https://spark.apache.org/docs/1.6.0/api/scala/index.html#org.apache.spark.rdd.RDD

EDIT:

always use .count() with .agg() while operating on groupedDataSet in order to avoid confusion in future:

empDF.groupBy($"department").agg(count($"department") as "countDepartment").show
