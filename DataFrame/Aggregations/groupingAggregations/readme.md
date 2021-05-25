**Group Aggregrations**
```
  import session.implicits._
  /*
   * Group the dataset on Country, InvoiceNo
   * Total Quantity for each group
   * Sum of each Invoice's Value
   */

  //Using Column Object
  val groupedDF1 = ordersDF.groupBy("Country", "InvoiceNo")
                   .agg(sum("Quantity").as("TotalQuantity"),
                    sum(expr("Quantity * UnitPrice")).as("SumofInvoiceValue"))
  groupedDF1.show()

  //Using Expressions
  val groupedDF2 = ordersDF.groupBy("Country", "InvoiceNo")
                    .agg(expr("sum(Quantity) as TotalQuantity"),
                    expr("sum(Quantity*UnitPrice) as SumofInvoiceValue"))
  groupedDF2.show()

  //Using SQL queries
  ordersDF.createOrReplaceTempView("orders")

  val query = """
              SELECT Country, InvoiceNo, sum(Quantity) as TotalQuantity, sum(Quantity*UnitPrice) as SumofInvoiceValue
              FROM orders
              GROUP BY Country, InvoiceNo
              """

  session.sql(query).show()
```
