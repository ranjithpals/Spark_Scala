**Simple Aggregations**
 - /*Find the following Aggregations
   * Number of Rows
   * Sum of Quantity
   * Avg Unit Price
   * Distinct Number of InvoiceNos
   */
```
  //Using Column Object
  ordersDF.select(
      count("*").as("RowCount"),
      sum("Quantity").as("TotalQuantity"),
      avg("UnitPrice").as("AvgUnitPrice"),
      countDistinct("InvoiceNo").as("DistinctInvoices")
  ).show()
  
  //Using SQL expressions
  ordersDF.selectExpr(
      "count(*) as RowCount",
      "sum(Quantity) as TotalQuantity",
      "avg(UnitPrice) as AvgUnitPrice",
      "count(distinct InvoiceNo) as DistinctInvoices"
  ).show()
  
  //Using SQL Queries
  ordersDF.createOrReplaceTempView("orders")
  
  val query = """
    select count(*) as RowCount, sum(Quantity) as TotalQuantity, 
    avg(UnitPrice) as AvgUnitPrice, count(distinct InvoiceNo) as DistinctInvoices
    from orders
    """
  session.sql(query).show()
```
