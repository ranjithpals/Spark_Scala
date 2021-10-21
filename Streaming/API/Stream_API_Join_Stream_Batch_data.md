**Input Streaming Data**

{"card_id":5572427538311236,"amount":5358617,"postcode":41015,"pos_id":970896588019984,"transaction_dt":"2018-10-21 04:37:42"}
{"card_id":5572427538311236,"amount":356492,"postcode":53149,"pos_id":160065694733720,"transaction_dt":"2018-09-22 05:21:43"}
{"card_id":5134334388575160,"amount":8241105,"postcode":13302,"pos_id":338546916831311,"transaction_dt":"2018-02-12 19:25:59"}
{"card_id":5134334388575160,"amount":3838236,"postcode":14008,"pos_id":974950096547483,"transaction_dt":"2018-07-17 23:13:13"}
{"card_id":5134334388575160,"amount":180659,"postcode":88030,"pos_id":712925555674508,"transaction_dt":"2018-11-18 10:42:00"}
{"card_id":5134334388575160,"amount":253503,"postcode":58501,"pos_id":140134748682132,"transaction_dt":"2018-07-20 05:47:42"}
{"card_id":5134334388575160,"amount":568232,"postcode":64743,"pos_id":91157863715349,"transaction_dt":"2018-02-22 14:10:45"}
{"card_id":5134334388575160,"amount":6263701,"postcode":40143,"pos_id":268682060701192,"transaction_dt":"2018-05-26 01:47:54"}
{"card_id":5134334388575160,"amount":9858073,"postcode":79082,"pos_id":99881011795155,"transaction_dt":"2018-09-29 20:31:25"}
{"card_id":5134334388575160,"amount":866874,"postcode":60958,"pos_id":671847163136812,"transaction_dt":"2018-11-29 09:51:17"}
{"card_id":5285400498362679,"amount":7982864,"postcode":92382,"pos_id":319523249711457,"transaction_dt":"2018-05-02 20:05:24"}
{"card_id":5285400498362679,"amount":2140506,"postcode":25040,"pos_id":762919765568319,"transaction_dt":"2014-01-08 03:31:52"}

**Logic for Joins when dealing with Streaming and Batch Data**

- Inner Join works with the Streaming dataset on either of the positions.
- Left Outer Joins works - Only when the Streaming data is on the **LEFT** and static data is on the **RIGHT**
- Right Outer Joins works - Only when the Streaming data is on the **RIGHT** and static data is on the **LEFT**
- Full Outer Join does not work

- The LEFT and RIGHT join works **similar to Map Side Join in MapReduce and BroadcastJoin in Spark**. 
- The **Smaller dataset is STATIC data** and the **Streaming data is Larger Dataset**





```
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
root
 |-- card_id: long (nullable = true)
 |-- amount: float (nullable = true)
 |-- post_code: integer (nullable = true)
 |-- pos_id: long (nullable = true)
 |-- transaction_dt: timestamp (nullable = true)
 |-- member_id: long (nullable = true)
 |-- card_issue_date: timestamp (nullable = true)
 |-- country: string (nullable = true)
 |-- state: string (nullable = true)

-------------------------------------------
Batch: 0
-------------------------------------------
+-------+------+---------+------+--------------+---------+---------------+-------+-----+
|card_id|amount|post_code|pos_id|transaction_dt|member_id|card_issue_date|country|state|
+-------+------+---------+------+--------------+---------+---------------+-------+-----+
+-------+------+---------+------+--------------+---------+---------------+-------+-----+

-------------------------------------------
Batch: 1
-------------------------------------------
+----------------+---------+---------+---------------+-------------------+---------------+-------------------+-------------+---------+
|         card_id|   amount|post_code|         pos_id|     transaction_dt|      member_id|    card_issue_date|      country|    state|
+----------------+---------+---------+---------------+-------------------+---------------+-------------------+-------------+---------+
|5572427538311236|5358617.0|     null|970896588019984|2018-10-21 04:37:42|976740397894598|2014-12-15 08:06:58|United States|Tonawanda|
+----------------+---------+---------+---------------+-------------------+---------------+-------------------+-------------+---------+

-------------------------------------------
Batch: 2
-------------------------------------------
+----------------+---------+---------+---------------+-------------------+---------------+-------------------+-------------+---------+
|         card_id|   amount|post_code|         pos_id|     transaction_dt|      member_id|    card_issue_date|      country|    state|
+----------------+---------+---------+---------------+-------------------+---------------+-------------------+-------------+---------+
|5572427538311236| 356492.0|     null|160065694733720|2018-09-22 05:21:43|976740397894598|2014-12-15 08:06:58|United States|Tonawanda|
|5134334388575160|8241105.0|     null|338546916831311|2018-02-12 19:25:59|978465390240911|2012-10-17 11:55:14|United States| Kankakee|
+----------------+---------+---------+---------------+-------------------+---------------+-------------------+-------------+---------+

-------------------------------------------
Batch: 3
-------------------------------------------
+----------------+---------+---------+---------------+-------------------+---------------+-------------------+-------------+--------+
|         card_id|   amount|post_code|         pos_id|     transaction_dt|      member_id|    card_issue_date|      country|   state|
+----------------+---------+---------+---------------+-------------------+---------------+-------------------+-------------+--------+
|5134334388575160|3838236.0|     null|974950096547483|2018-07-17 23:13:13|978465390240911|2012-10-17 11:55:14|United States|Kankakee|
+----------------+---------+---------+---------------+-------------------+---------------+-------------------+-------------+--------+

-------------------------------------------
Batch: 4
-------------------------------------------
+----------------+---------+---------+---------------+-------------------+---------------+-------------------+-------------+---------+
|         card_id|   amount|post_code|         pos_id|     transaction_dt|      member_id|    card_issue_date|      country|    state|
+----------------+---------+---------+---------------+-------------------+---------------+-------------------+-------------+---------+
|5134334388575160| 180659.0|     null|712925555674508|2018-11-18 10:42:00|978465390240911|2012-10-17 11:55:14|United States| Kankakee|
|5134334388575160| 568232.0|     null| 91157863715349|2018-02-22 14:10:45|978465390240911|2012-10-17 11:55:14|United States| Kankakee|
|5134334388575160|9858073.0|     null| 99881011795155|2018-09-29 20:31:25|978465390240911|2012-10-17 11:55:14|United States| Kankakee|
|5285400498362679|7982864.0|     null|319523249711457|2018-05-02 20:05:24|978786807247400|2014-01-08 03:31:52|United States|Fallbrook|
|5134334388575160| 253503.0|     null|140134748682132|2018-07-20 05:47:42|978465390240911|2012-10-17 11:55:14|United States| Kankakee|
|5134334388575160|6263701.0|     null|268682060701192|2018-05-26 01:47:54|978465390240911|2012-10-17 11:55:14|United States| Kankakee|
|5134334388575160| 866874.0|     null|671847163136812|2018-11-29 09:51:17|978465390240911|2012-10-17 11:55:14|United States| Kankakee|
+----------------+---------+---------+---------------+-------------------+---------------+-------------------+-------------+---------+

-------------------------------------------
Batch: 5
-------------------------------------------
+----------------+---------+---------+---------------+-------------------+---------------+-------------------+-------------+---------+
|         card_id|   amount|post_code|         pos_id|     transaction_dt|      member_id|    card_issue_date|      country|    state|
+----------------+---------+---------+---------------+-------------------+---------------+-------------------+-------------+---------+
|5285400498362679|2140506.0|     null|762919765568319|2014-01-08 03:31:52|978786807247400|2014-01-08 03:31:52|United States|Fallbrook|
+----------------+---------+---------+---------------+-------------------+---------------+-------------------+-------------+---------+
```





