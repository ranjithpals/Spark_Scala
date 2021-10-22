Spark Streaming Session - 9
============================

batch processing - rdds to dataframes.

normal spark streaming to structured streaming.

Batch processing ->

capture the data and save it in a file.

we used to process this file at a regular interval/frequency lets say 12 hours.

15 mins.. -> schedule batch job every 15 mins

1 min... -> schedule the batch job every 1 min..

we want to reduce the frequency of processing.

Data Lake -> S3 , HDFS

Consider a big retail chain Big Bazar..

lot of outlets/stores in various cities..

500 different stores across the country.

in each store there will be 10 billing counters.

10 am - 9 pm open timings

every 15 mins we want to calculate the total sales across all the stores of big bazar.

every 15 minutes lets say we get a new file in our data lake

10:00 - 10:15 - 2 lakh
10:15 - 10:30 - 5 lakh
10:30 - 10:45 - 8 lakh
10:45 - 11:00 - 9 lakh

whatever transactions happen from 10 am to 10:15 am - there is no guarantee that during our 10:15 batch it will be processed.

for a transaction to reach from outlet to a distributed data store like hdfs will take time.

some of the records might arrive late..

10:10 (when the order was placed) -> 10:18 (reached to data lake)

at 10:30 am you want to execute batch 2.

what if the batch 1 is still running?

what if one of the batch fails how to handle that?

stream processing is more complicated than batch processing.

Reducing duration between the batches is what we are looking at.

micro batch processing.

spark streaming takes care of all the issues we talked about.

1. automatic looping between the batches.

2. storing intermediate results..

3. combining results to previous batch result.

4. restart from same place in case of any failure - fault tolerance.



spark streaming (Dstream)

built on top of Rdd's

we were using lower level constructs.

Limitations
============

1. Lack of spark sql engine optimization

2. only support processing time semantics and do not support event time semantics..

invoice was created at 10:10 - event time.

processing time is the time when this event reached spark..

10:18 - processing time.

3. No further upgrades and enhancements expected.



spark structured streaming API
===============================

1. offers unified model for batch and stream processing.

2. runs over the spark sql engine. Takes advantage of the sql based optimizations.

3. native support for event time semantics.

4. further enhancements are expected.


Spark Streaming Session - 10
=============================

generate some words through a netcat utility..

nc -lk 9999

> hello how are you
> hello hello


spark structured streaming code which reads from the above socket and calculates the frequency of each word.

value
======
hello hello how are you


explode(split(value,' ')) as word)

split will always give you an array.

we now want to break this array in multiple rows.

Split -> [hello, hello, how, are, you]

explode on the array to get each word in a different row.

hello
hello
how
are
you

when you use select then you can just use columns of dataframes.

but when we use selectExpr then we can write sql functions in that.

word
======
hello
hello
how
are
you



Spark Streaming Session - 11
=============================

DataStream writer allows 3 output modes:

1. append

2. update - insert and update

3. complete - from starting till the end.


hello how are you

(hello,1)
(how,1)
(are,1)
(you,1)

hello sumit this side
(sumit,1)
(this,1)
(side,1)

that's why when we do aggregations append mode is not allowed.

spark is intelligent enough to give error when it sees that operation does not make sense.

Analysis exception - append output mode is not supported with streaming aggreagations.

update mode output is

+-----+-----+
| word|count|
+-----+-----+
|sumit|    1|
| this|    1|
| side|    1|
|hello|    2|
+-----+-----+


complete mode output is

+-----+-----+
| word|count|
+-----+-----+
|sumit|    1|
| this|    1|
| side|    1|
|hello|    2
How.  |.   1
are        1
you.       1
+-----+-----+


A spark application should run forever..

1. we manually stop it - kill it (maintainance)
2. Exception

we want our spark streaming application to stop and then restart gracefully.

whenever we call start()

spark driver will take the code from readStream to writeStream
and submits it to spark sql engine.

what spark sql engine will do?

analyse it, optimize it and then compile it to generate the execution plan.

execution plan is made of stages and tasks.

spark will then start a background thread to execute it.

and this thread will start a new job to repeat the processing and writing operations..

each job is a micro batch.

loop is created and managed by the background process/thread.

This goes on forever.

we have seen that whenever we type some words in terminal, then in spark UI we see a new job created.

whats the triggering point of a new micro batch?

trigger for each record or is it time based?

Data Stream writer allows us to define the trigger.



Spark Streaming Session - 12
=============================

when is a new micro batch triggered..

we have to loop over micro batches..

Data Stream writer allows us to define the trigger...

4 trigger configurations
==========================

1. unspecified - New microbatch is triggered as soon as current microbatch finishes... . However microbatch will wait for new input data...


2. time interval - example 5 mins.. first trigger will start immediately. second microbatch will begin when the first mircobatch is finished and the time is elapsed...

5 mins

case when the first microbatch finishes in less than 5 mins
=============================================================
the first microbatch finishes in 15 seconds

the second mircobatch has to wait for 4 mins 45 seconds to get triggered.

case when the first microbatch takes more than 5 mins to process
==================================================================
if there is any new data then the new microbatch will trigger immediately after the first microbatch is finished.


3. one time - like batch processing.

you can spin up a cluster in cloud and process this data.

this will keep at track of all what is processed earlier and this will only process the new data..

it will maintain the previous state and will keep a track of data which is already processed.


4. continuous - expiremental (milli second latency)


Spark Streaming Session - 13
=============================

Spark streaming offers 4 built in data sources.

1. socket source - you read the streaming data from a socket (IP + port).

if your producer is producing at a very fast pace and writing to socket.

if your consumer is reading from the above socket at a slower pace then we can have data loss.

This socket source is not meant for production use cases, as we can end up with data loss.

2. Rate source - used for testing and benchmarking purpose.

if you want to generate some test data then you can generate a configurable number of key value pairs.

3. File source - you will basically have a folder (directory)

new files are detected in this directory and processed.

4. Kafka source - we will talk about it later when we learn Kafka..


I will be getting new files in my input folder..

and my task is to filter the completed orders and put it in a output file.

options
========

.option("maxFilesPerTrigger",1)

that over the time the number of files in the directory will keep on increasing.

if we have a lot of files in folder then it will keep on becoming difficult or time consuming.

that is why it is recommended to clean up the older files as and when required..

Option - cleanSource and sourceArchiveDir are often used together.

cleanSource takes 2 values archive and delete

.option("cleanSource","delete")

.option("cleanSource","archive")
.option("sourceArchiveDir","Name-of-archive-dir")

Remember that going with any of the above will increase the processing time.

if your expected time to process is very quick then you should avoid using any of the above.

In such cases you can have your own batch job scheduled which will take care of this cleanup.

if your older processed files are not removed from the folder then it will adversely affect the performance.



Spark Streaming Session - 14
=============================

File data source

Fault tolerance and exactly once Guarantee
============================================

Ideally a streaming application should run forever.

1. Exception
2. Maintainance activities

our application should be able to stop and restart gracefully.

This means to maintain exactly once semantics..

1. Do not miss any input record.
2. Do not create duplicate output records.

Spark Structured steaming provides ample support for this.

It maintains state of the microbatch in the checkpoint location.

Checkpoint location helps you to achieve fault tolerance.

Checkpoint location mainly contains 2 things:

1. read positions - which all files are already processed

2. state information - running total

Spark Structured streaming maintains all information it requires to restart the unfinished microbatch.

to guarantee exactly once 4 requirements should be met -

1. restart application with same checkpoint location - lets say in 3rd microbatch we got an exception..

in commits we would have 2 commits.

2. use a replayable source - consider there are 100 records in 3rd microbatch.

after processing 30 records it gave some exception.

these 30 records should be availble to you when you start reprocessing.

replayable means we can still get the same data which is processed earlier.

when we are using socket source then we cannot get older data back again. so socket source is not replayable.

kafka, file source both of them are replayable.


3. use deterministic computation - lets say our 3rd microbatch has 100 records.

while processing 30th record we got an error.

whenever we start again it will start from beginning of 3rd microbatch and these 30 records are also processed again.

The output should remain same.


sqrt(4) - deterministic

dollarsToRs(10) - 720 (not deterministic)
- 738



4. use an idempotent sink -

lets say our 3rd microbatch has 100 records.

while processing 30th record we got an error.

whenever we start again it will start from beginning of 3rd microbatch and these 30 records are also processed again.

dont you feel that we are processing these 30 records 2 times?

we are writing this to output 2 times..

2nd time when you are writing the same output it should not impact us.

Either it should discard the 2nd output or it should overwrite the 1st ouput with the 2nd one..

sources - socket, file, kafka

sink - console, file, kafka

output modes - append, complete, update

triggers - unspecified, time interval,one time, continuous

checkpoint - state information, read information

fault tolerance - exactly once guarantee (checkpoint and we need to follow the 4 above conditions)


Spark Streaming Session - 15
=============================

Stateful vs Stateless transformations.

stateless
==========
select, filter, map, flatMap, explode

these transformations work only with the current microbatch and have nothing to do with the history.

Stateful
=========
Last state is maintained in a state store.

grouping and aggregations cannot work without history.
so these kind of transformations rely on state informaiton or the history.

Note - stateless transformation do not support complete output mode.

for example

map, select..

10000 input -> 10000 output

remembering the state of the query is not effecient.


when we are using stateful transformations we are storing the state (history) in state store.

excessive state can cause out of memory exception.

Because the state is maintained in the memory of the spark executors.

checkpoint directory.. (position + state information)

ideally it will be stored on hdfs.

state information is stored in checkpoint directory to handle fault tolerance.

same state information which is present in this file will be maintained in executor memory for quick lookups.

we should be careful when we deal with stateful operations because if the history or the state keeps on growing it can lead to out of memory error.

Spark offers us 2 kinds of stateful operations.

1. Managed stateful operations - spark manages on how to clean up the state store.

2. unManaged stateful operations - where we as developer decide on when to cleanup the state store. we write our custom cleanup logic.
scala/java but still it's not present in python.


Aggregations are of 2 types..
==============================

1. continuous aggregations -
you purchase your grocery from Big Bazar

on purchase of 100 Rs you get 1 reward point.

these reward points never expire. (Unbounded)

100 million customers..

every month they are getting 1 million new customers.

history in state store will keep on growing.

In such cases where we have unbounded things we can think of unmanaged stateful operations to do the cleanup.

2. time bound aggregations - one month window. The reward points expire after one month.

In such cases spark can help us to clean the state store. because any data before 1 month can be cleaned up.

Managed stateful operations where spark will work on the cleanup logic.

spark can clear the state for previous month and start again.

so all the time bound aggregation are best for spark to manage the cleanup.

Time bound aggregates are also known as window aggregates.

There are 2 types of windows
=============================

1. Tumbling time window - a series of fixed size, non overlapping time interval.
10 - 10:15
10:15 - 10:30
10:30 - 10:45
10:45 - 11


2. Sliding time window - a series of fixed size, overlapping window.
10 - 10:15
10:05 - 10:20
10:10 - 10:25


The window size is 15 minutes and the sliding interval is 5 minutes.



Spark Streaming Session - 16
=============================

The aggregation windows are based on event time and not on trigger time.

Big Bazar

{"order_id":57012,"order_date":"2020-03-02 11:05:00","order_customer_id":2765,"order_status":"PROCESSING", "amount": 200}

event time is 11:05

this event might reach to spark lets say at 11:20


Amount of sales every 15 minutes..

Tumbling window

11 - 11:15
11:15 - 11:30
11:30 - 11:45
11:45 - 12


lets say event time is 11:14 (the order was placed at 11:14)

this order reached quite late to spark lets say at 11:50

what happens to late records...


The state store maintains this info
====================================

2020-03-02 11:00:00|2020-03-02 11:15:00|         900|
2020-03-02 11:15:00|2020-03-02 11:30:00|        1500|
2020-03-02 11:30:00|2020-03-02 11:45:00|         500|
2020-03-02 11:45:00|2020-03-02 12:00:00|         400|

Late arriving records update the state store.


if you keep on enternaining the late arriving records in a unbounded way.

if we want to deal with late coming records. Then we wont be able to clean the state store.  And we have to maintain all the state.

The reason we have to maintain the state is to tackle late coming records.





Spark Streaming Session - 17
=============================

To deal with late coming records.

There is a concept with the name watermark

what is a watermark?

it is like setting an expiry date to a record..

The business says that we are looking for 99.9% accuracy.

99.9% of your records are never late than 30 minutes

.1% of the transactions we might receive after 30 minutes

1000 events... 1 event can arrive later than 30 minutes..

999 events arrive before 30 minutes..

in such a senario we can safely discard the event which arrive later than 30 minutes..

the state information should not be maintained for windows which are before the watermark duration.

State Store
==============

2020-03-02 11:15:00|2020-03-02 11:30:00|        1500|
2020-03-02 11:30:00|2020-03-02 11:45:00|         500|
2020-03-02 11:45:00|2020-03-02 12:00:00|         400|



watermark boundary = max(event time) - watermark = 10:48

11:48 - 30 mins = 11:16

each microbatch ends up doing 3 things -

1. update the state store
2. cleanup the state store
3. send the output to the sink.

Remember that

1. watermark is the key to clean our state store.
2. events within he watermark are taken - this is guaranteed.
3. events outside the watermark may or may not be taken.

Spark Streaming Session - 18
=============================

State store cleanup
===================

1. setting a watermark

2. using an appropriate output mode.

we have 3 different output modes -

1. complete
2. update
3. append

complete ouput mode will not allow us to clean the state store.

complete mode for windowing aggregates has side effect on state store cleaning.

update mode is the most useful and efficient mode.

append mode..

append mode is not allowed on window aggregates.

that I will work when we are sure that there are no updates.

so when records arrive late we have to update the previous windows.

when we talk about watermarks the scenario is now changed.

spark allows to use append mode when we are using watermarks on event time..

it will wait to output the results to the sink..

11:12 - 30 mins = 10:42

11 - 11:15

this window information will only be printed once this window expired.

watermark boundary is now 11:10

append mode can work when we set watermarks but the only downside is that we have to wait atleast for the watermark duratiton (whenever the window expires then only it will ouput the results to the sink)

append mode will supress the output of window aggregates unless they cross watermark boundary.



Sliding window
===============

11 - 11:15
11:05 - 11:20



Spark Streaming session - 19
==============================

Streaming joins..

Structured streaming supports 2 kind of joins.

1. Streaming Dataframe to static dataframe.

2. streaming dataframe with another streaming dataframes.


Streaming Dataframe to static dataframe
========================================

Stream enrichments..

Lets consider an example from banking domain.

we swipe our card at some pos terminals..

whenever I swipe my card a transaction will be generated..

JSON data...

card_id, amount, postal code, pos_id, transaction time.

Streaming dataframe
====================
{"card_id":5572427538311236,"amount":5358617,"postcode":41015,"pos_id":970896588019984,"transaction_dt":"2018-10-21 04:37:42"}

we have to enrich this transaction in real time..

member_id , country, state, card_release_time

Static dataframe
====================
card_id,member_id,card_issue_date,country,state
5572427538311236,976740397894598,2014-12-15 08:06:58.0,United States,Tonawanda


Spark Streaming session - 20
=============================

Streaming DF with static DF - inner Join

Left outer join is possible

Right outer Join is possible

Full outer join is not supported..

Left outer join is possible
============================

only if left side is a stream and right side is static df.


Right outer join is possible
============================

only if right side is a stream and left side is static.


streaming

{"card_id":5572427538311236,"amount":5358617,"postcode":41015,"pos_id":970896588019984,"transaction_dt":"2018-10-21 04:37:42"}
{"card_id":5572427538311236,"amount":356492,"postcode":53149,"pos_id":160065694733720,"transaction_dt":"2018-09-22 05:21:43"}

{"card_id":55724275383112300,"amount":5358617,"postcode":41015,"pos_id":970896588019984,"transaction_dt":"2018-10-21 04:37:42"}


static

5572427538311236,976740397894598,2014-12-15 08:06:58.0,United States,Tonawanda
5134334388575160,978465390240911,2012-10-17 11:55:14.0,United States,Kankakee
5285400498362679,978786807247400,2014-01-08 03:31:52.0,United States,Fallbrook


Left outer Join
================
all the matching records + all the non matching records from left table padded with nulls on the right...


Map side join works...

if left table is small and right table is bigger than right outer is possible...

if right table is small and left table is bigger than left outer join is possible..


in hive the small table is equivalent to the static dataframe in spark


the left table is :refinedTransactionDf (streaming)

the right table is :memberDF (static)


streaming df on left and static on the right...

leftouter is possible.

rightouter is not possible.

Right outer join with a streaming DataFrame/Dataset on the left and a static DataFrame/DataSet on the right not supported;



Spark Streaming session - 21
=============================

stream to static join are stateless - as one side of the join is completely knows in advance.

So there is no need to maintain any state.

Stream to Stream join - this is a stateful join..

the state has to be maintained in the state store...

The challenge of generating join results between two data streams is that,

at any point of time, the view of the dataset is incomplete for both the sides of the join.

This is make it very hard for us to find matches between inputs.

Both the input streams have to be buffered in the state store..

It will continously check for a match when new data arrives.

ideally the best practise is to clean the state store. else we will run into out of memory issues.

Inner Join
==========

matching records from both the tables..

if lets say 100 people view the ad

then impression is 100

out of 100 people who viewed might be lets 3 people click on it.

clicks - 3

3%

There are 2 kind of events that are happening

1. impression

2. click


We need to make sure we clean the state store to avoid out of memory issues.

watermark


Spark Streaming session - 22
=============================

a streaming and a static

one streaming + one static df - inner

left outer and right outer were possible.

left outer - streaming df should on the left side

right outer - streaming df should be on the right side.


when both the dataframes are streaming
=======================================

inner - recommended to use watermark for state store clean.

Left outer Join -
1. there should be watermark on the right side stream.
2. Maximum time constraint between the left and the right side streams.

for example if a impression happens at 10 pm

then a click on that before 10:15 pm will only be considered..


Right outer Join-
1. there should be watermark on the left side stream.
2. Maximum time constraint between the left and the right side streams.

for example if a impression happens at 10 pm

then a click on that before 10:15 pm will only be considered..