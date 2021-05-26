**DataFrame Joins**
```
  //Join the orders with customers using Right join to show all customers irrespective of placing an order or NOT and order by order_id
  val joinType2 = "right"
  val joinCondition2 = ordersDF.col("order_customer_id") === customersDF.col("customer_id")
  val rightJoin = ordersDF.join(customersDF, joinCondition2, joinType2).orderBy("order_id")
  rightJoin.show()
```
