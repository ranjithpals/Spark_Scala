**DIFFERENT OVERLOADING OF SELECT STATEMENT (METHOD) IN DATAFRAME's**
```
  //Select columns -> Column String
  ordersDF.select("order_id", "order_status").show()

  //Select Columns -> Column Object: Scala Specific syntactic sugar
  ordersDF.select($"order_id", 'order_status).show()

  //Select Columns -> Column Object: column("<name>") or col("<name>")
  ordersDF.select(column("order_id"), col("order_customer_id")).show()

  //Select Columns -> Column Expressions
  ordersDF.select("concat(order_status, '_STATUS')").show()
```
- Within the same DF.select statement.
> We cannot use Column String and Column Object
> We cannot use Column String with Column Expression
> We cannot use Column object with Column Expression

- Column Expression can be converted to Column Object using **expr** function
```
  //Select Columns -> Convert Column Expressions to Column Object
  ordersDF.select(column("order_id"), expr("concat(order_status, '_STATUS')")).show()
```
- Instead of converting all columns to Column Objects we can use **.selectExpr(<col strings>)**
```
  //Select Columns -> Use .selectExpr to use all col strings along with expressions without explicit conversions
  ordersDF.selectExpr("order_id", "order_date", "concat(order_status, '_STATUS')").show(false)
```
**RENAME COLUMN**
```
 val ordersNew = ordersDF.withColumnRenamed("customer_id", "cust_id")
```
**DROP COLUMN**
- Example here is after performing a join operation, so the column needs a table reference
```
 val ordersNew = ordersDF.withColumnRenamed("customer_id", "cust_id")
```
**COALESCE FUNCTION**
```
  //Replace Null values with a common representation for such values
  val outerjoin3 = outerJoin2
  .select("order_id", "customer_fname", "customer_id", "order_status")
  .sort("order_id")
  .withColumn("order_id", expr("coalesce(order_id, -1)"))
  .show(100)
```
