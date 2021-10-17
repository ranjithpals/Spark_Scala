Console Input
================
[itv001157@g01 ~]$ nc -lk 9992

{"order_id":57012,"order_date":"2020-03-02 11:05:00","order_customer_id":2765,"order_status":"PROCESSING", "amount": 200}
{"order_id":45256,"order_date":"2020-03-02 11:12:00","order_customer_id":791,"order_status":"COMPLETE", "amount": 300}
{"order_id":53974,"order_date":"2020-03-02 11:20:00","order_customer_id":2098,"order_status":"COMPLETE", "amount": 700}
{"order_id":5335,"order_date":"2020-03-02 11:40:00","order_customer_id":5547,"order_status":"ON_HOLD", "amount": 500}
{"order_id":29288,"order_date":"2020-03-02 11:25:00","order_customer_id":3943,"order_status":"CLOSED", "amount": 200}
{"order_id":5852,"order_date":"2020-03-02 11:48:00","order_customer_id":9344,"order_status":"COMPLETE", "amount": 400}
{"order_id":5852,"order_date":"2020-03-02 11:14:00","order_customer_id":9344,"order_status":"COMPLETE", "amount": 400}
{"order_id":5852,"order_date":"2020-03-02 11:16:00","order_customer_id":9344,"order_status":"COMPLETE", "amount": 600}

Code Logic
============
**WATERMARK**
- Watermark is like setting a **expiry datetime** to an incoming record applied when we transform the record
- Based on the requirement from the Business, on when the majority of the records are expected, the watermark is applied.
- **Max(watermark column) - WaterMark bandwith** (30 minute in this example) is calculated and any **Window behind the calculated value is Ignored**
- The First 6 rows of the example - creates 3 batches 11 to 1115, 1115 to 1130, 1130 to 1145
- The 7th Row when added will remove the 11 to 1115 batch as 11:48 - 30min = 11:18 and there is one batch behind the 11:18 timeline.
- The 11 to 1115 batch will be removed from the state store.

**Each Microbatch ends up doing 3 things**
- Update the State Store.
- Clean up the State Store.
- Send output to the Sink.

**Key things about WATERMARK**
- Watermark is the key to clean up the state store.
- Events within the watermark are considered for results - this is guaranteed.
- Events outside the watermark may or may not be considered.
Console Output
================
```
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
root
 |-- start: timestamp (nullable = true)
 |-- end: timestamp (nullable = true)
 |-- total_amount: double (nullable = true)

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
-------------------------------------------
+-----+---+------------+
|start|end|total_amount|
+-----+---+------------+
+-----+---+------------+

-------------------------------------------
Batch: 3
-------------------------------------------
+-------------------+-------------------+------------+
|              start|                end|total_amount|
+-------------------+-------------------+------------+
|2020-03-02 11:00:00|2020-03-02 11:15:00|       500.0|
+-------------------+-------------------+------------+

-------------------------------------------
Batch: 4
-------------------------------------------
+-----+---+------------+
|start|end|total_amount|
+-----+---+------------+
+-----+---+------------+

-------------------------------------------
Batch: 5
-------------------------------------------
+-------------------+-------------------+------------+
|              start|                end|total_amount|
+-------------------+-------------------+------------+
|2020-03-02 11:15:00|2020-03-02 11:30:00|       700.0|
+-------------------+-------------------+------------+

-------------------------------------------
Batch: 6
-------------------------------------------
+-------------------+-------------------+------------+
|              start|                end|total_amount|
+-------------------+-------------------+------------+
|2020-03-02 11:30:00|2020-03-02 11:45:00|       500.0|
+-------------------+-------------------+------------+

-------------------------------------------
Batch: 7
-------------------------------------------
+-----+---+------------+
|start|end|total_amount|
+-----+---+------------+
+-----+---+------------+

-------------------------------------------
Batch: 8
-------------------------------------------
+-------------------+-------------------+------------+
|              start|                end|total_amount|
+-------------------+-------------------+------------+
|2020-03-02 11:15:00|2020-03-02 11:30:00|       900.0|
+-------------------+-------------------+------------+

-------------------------------------------
Batch: 9
-------------------------------------------
+-------------------+-------------------+------------+
|              start|                end|total_amount|
+-------------------+-------------------+------------+
|2020-03-02 11:45:00|2020-03-02 12:00:00|       400.0|
+-------------------+-------------------+------------+

-------------------------------------------
Batch: 10
-------------------------------------------
+-----+---+------------+
|start|end|total_amount|
+-----+---+------------+
+-----+---+------------+

-------------------------------------------
Batch: 11
-------------------------------------------
+-----+---+------------+
|start|end|total_amount|
+-----+---+------------+
+-----+---+------------+

-------------------------------------------
Batch: 12
-------------------------------------------
+-------------------+-------------------+------------+
|              start|                end|total_amount|
+-------------------+-------------------+------------+
|2020-03-02 11:15:00|2020-03-02 11:30:00|      1500.0|
+-------------------+-------------------+------------+
```