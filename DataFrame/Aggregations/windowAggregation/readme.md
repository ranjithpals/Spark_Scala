**Window Aggregations**
- When defining any window function, need 3 things defined within
> PartitionBy
> OrderBy
> RowsBetween
- Aggregation function .over(windowfunc)

```
//Define the Window Function
  val windowfunc = Window.partitionBy("country")
                          .orderBy("weeknum")
                          .rangeBetween(Window.unboundedPreceding, Window.currentRow)

  val finalDF = invoiceDF.withColumn("RunningTotal", sum("invoicevalue").over(windowfunc))
```
