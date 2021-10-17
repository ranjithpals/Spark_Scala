
Stream Input
=============

[itv001157@g01 ~]$ nc -lk 9990

{"order_id":57012,"order_date":"2020-03-02 11:05:00","order_customer_id":2765,"order_status":"PROCESSING", "amount": 200}
{"order_id":45256,"order_date":"2020-03-02 11:12:00","order_customer_id":791,"order_status":"COMPLETE", "amount": 300}
{"order_id":53974,"order_date":"2020-03-02 11:20:00","order_customer_id":2098,"order_status":"COMPLETE", "amount": 700}
{"order_id":5335,"order_date":"2020-03-02 11:40:00","order_customer_id":5547,"order_status":"ON_HOLD", "amount": 500}
{"order_id":29288,"order_date":"2020-03-02 11:25:00","order_customer_id":3943,"order_status":"CLOSED", "amount": 200}
{"order_id":5852,"order_date":"2020-03-02 11:48:00","order_customer_id":9344,"order_status":"COMPLETE", "amount": 400}
{"order_id":5852,"order_date":"2020-03-02 11:14:00","order_customer_id":9344,"order_status":"COMPLETE", "amount": 400}


Logic Used
==============
- Four Windows of 15 min duration are created based on the Input starting from 11:00 to 12:00
- 11 to 11:15
- 11:15 to 11:30
- 11:30 to 11:45
- 11:45 to 12
- Aggregation of amount field is done for event trigger (order_date) values falling between these windows
- But over a period of time these checkpointlocation details can really swell causing memory issues

Checkpoint location
====================
- The checkpointLocation data is written to HDFS lying within the Node Off-Heap Memory.
- The same data is also held within the Heap-memory of the Executor.
- If the data gets increased over a period of time based on the micro-batches it creates "Out of Memory" issues.
- This needs to be resolved either using Spark Managed cleanup or UnManaged cleanup.

Deal with Late Arriving Records
================================
- If we keep entertaining late arriving records (ex. records arrive after many days or months since event time (in this example) in a unbounded way it will lead to issues
- We would not be able to clean the state, and will have to maintain all the states since the beginning.
- We would need to tackle this.

Console Output
==============
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
root
 |-- start: timestamp (nullable = true)
 |-- end: timestamp (nullable = true)
 |-- total_amount: double (nullable = true)

```
-------------------------------------------
Batch: 0
-------------------------------------------
+-----+---+------------+
|start|end|total_amount|
+-----+---+------------+
+-----+---+------------+
-------------------------------------------
Batch: 1
-------------------------------------------
+-------------------+-------------------+------------+
|              start|                end|total_amount|
+-------------------+-------------------+------------+
|2020-03-02 11:00:00|2020-03-02 11:15:00|       200.0|
+-------------------+-------------------+------------+

-------------------------------------------
Batch: 2
+-------------------+-------------------+------------+
|              start|                end|total_amount|
+-------------------+-------------------+------------+
|2020-03-02 11:00:00|2020-03-02 11:15:00|       500.0|
+-------------------+-------------------+------------+

-------------------------------------------
Batch: 3
-------------------------------------------
+-------------------+-------------------+------------+
|              start|                end|total_amount|
+-------------------+-------------------+------------+
|2020-03-02 11:15:00|2020-03-02 11:30:00|       700.0|
+-------------------+-------------------+------------+

-------------------------------------------
Batch: 4
-------------------------------------------
+-------------------+-------------------+------------+
|              start|                end|total_amount|
+-------------------+-------------------+------------+
|2020-03-02 11:30:00|2020-03-02 11:45:00|       500.0|
+-------------------+-------------------+------------+

-------------------------------------------
Batch: 5
-------------------------------------------
+-------------------+-------------------+------------+
|              start|                end|total_amount|
+-------------------+-------------------+------------+
|2020-03-02 11:15:00|2020-03-02 11:30:00|       900.0|
+-------------------+-------------------+------------+

-------------------------------------------
Batch: 6
-------------------------------------------
+-------------------+-------------------+------------+
|              start|                end|total_amount|
+-------------------+-------------------+------------+
|2020-03-02 11:45:00|2020-03-02 12:00:00|       400.0|
+-------------------+-------------------+------------+

-------------------------------------------
Batch: 7
-------------------------------------------
+-------------------+-------------------+------------+
|              start|                end|total_amount|
+-------------------+-------------------+------------+
|2020-03-02 11:00:00|2020-03-02 11:15:00|       900.0|
+-------------------+-------------------+------------+
```
