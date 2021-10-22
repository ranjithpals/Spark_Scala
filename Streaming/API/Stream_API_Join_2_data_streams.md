
**Input - Read Stream data from Console**

_Impression_data_

1
{"impressionID": "100001", "ImpressionTime": "2020-11-01 10:00:00", "CampaignName": "Trendytech India"}
{"impressionID": "100002", "ImpressionTime": "2020-11-01 10:00:00", "CampaignName": "Trendytech India"}
2
{"impressionID": "100003", "ImpressionTime": "2020-11-01 10:46:00", "CampaignName": "Trendytech India"}

_Click_data_

1
{"clickID": "100001", "ClickTime": "2020-11-01 10:15:00"}
2
{"clickID": "100003", "ClickTime": "2020-11-01 11:00:00"}
3
{"clickID": "100002", "ClickTime": "2020-11-01 10:15:00"}
{"clickID": "100001", "ClickTime": "2020-11-01 10:15:00"}

**Logic of Script to join 2 Streaming Inputs**

1. Two Streaming data can happen only for 'append' OutputMode
2. When the records gets expired based on the WaterMark date field, the join does not occur even if the records in the other source exists.
3. The data is entered as indicated by the numbers above.


**Output - Read and Join data from TWO Streams**

```
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
root
 |-- impressionID: string (nullable = true)
 |-- ImpressionTime: timestamp (nullable = true)
 |-- CampaignName: string (nullable = true)
 |-- ClickTime: timestamp (nullable = true)

-------------------------------------------
Batch: 0
-------------------------------------------
+------------+--------------+------------+---------+
|impressionID|ImpressionTime|CampaignName|ClickTime|
+------------+--------------+------------+---------+
+------------+--------------+------------+---------+

-------------------------------------------
Batch: 1
-------------------------------------------
+------------+--------------+------------+---------+
|impressionID|ImpressionTime|CampaignName|ClickTime|
+------------+--------------+------------+---------+
+------------+--------------+------------+---------+

-------------------------------------------
Batch: 2
-------------------------------------------
+------------+-------------------+----------------+-------------------+
|impressionID|     ImpressionTime|    CampaignName|          ClickTime|
+------------+-------------------+----------------+-------------------+
|      100001|2020-11-01 10:00:00|Trendytech India|2020-11-01 10:15:00|
+------------+-------------------+----------------+-------------------+

-------------------------------------------
Batch: 3
-------------------------------------------
+------------+-------------------+----------------+-------------------+
|impressionID|     ImpressionTime|    CampaignName|          ClickTime|
+------------+-------------------+----------------+-------------------+
|      100003|2020-11-01 10:46:00|Trendytech India|2020-11-01 11:00:00|
+------------+-------------------+----------------+-------------------+

-------------------------------------------
Batch: 4
-------------------------------------------
+------------+--------------+------------+---------+
|impressionID|ImpressionTime|CampaignName|ClickTime|
+------------+--------------+------------+---------+
+------------+--------------+------------+---------+
```
