### OUTPUT MODE
- **Append** - No further updates can be performed on the published data, forgets what was processed in the previous micro-batches.
- **Update** - Updates can be performed to existing records and New records can be published, remembers the results of the previous micro-batch (checkpointLocation)
- **Complete** - All the results are published since the start of the Streaming Session.


### TRIGGERS
- **Unspecified** - Batch is created when a new record/file is produced in the source
- **Time Interval** - Batch is created upon completion of the current batch + (followed by) the time specified as the interval. 
- **One-Time** - Similar to Batch, like runs once in a day and still remembers the state (previous runs details) before running the next time.
- **Continous** - Still under development
### CONFIGURATIONS - Additional
- session.config("spark.streaming.stopGracefullyOnShutdown", "true")
- session.config("spark.sql.shuffle.partitions", 3)
- streamWriter.trigger(Trigger.ProcessingTime("10 seconds")) - Triggers Writes once in specified time interval
- streamReader.option("maxFilesPerTrigger", 1) - Creates Only one file per Trigger Session.
### STREAM INPUT - FILE SOURCE
- val df1 = spark.readStream.format("json").option("path", "InputFolder")
- val writeDf = completedOrders.writeStream.format("json").option("path", "OutputFolder")
**Archive**
- In the case of files being received every few seconds, after a point of time the 
number of files in the directory will be keep increasing.
- If we have a lot of files in folder then it will slow down the read of the subsequent files.
- It is recommended to clean up the older files when possible.
- Options to achieve the archive of the files are
  1. cleanSource & sourceArchiveDir are often used together
  2. .option("cleanSource", "delete") - DELETE THE FILES after they are consumed.
  3. .option("cleanSource", "archive") & .option("sourceArchiveDir", "Name-of-archive-dir")
  4. In the above option, the files are archived immediately but it slows down the job run, if there is need for quick processing then this need to be avoided.
  5. In such cases you can have your own batch job scheduled which will take care of this cleanup.
### Exactly Once Semantics ###
- To achieve the Exactly Once Semantic we need to satisfy the following requirements
1. Restart application with same Checkpoint location
2. Use a Replayable source (File Source, Kafka) unlike Console
3. Use Deterministic Computation - Key Identifier upon which the record is identified in the output(Sink) should be the same and should not have varying values
4. Use a Idempotent Sink - If we process same data once again, the duplicate data should not be written to a Output source, it should either be removed or overwritten 

### WATERMARK #### https://github.com/ranjithpals/Spark_Scala/blob/master/Streaming/API/Stream_API_TumblingWindow_Watermark.md


Output Mode  | Window Aggregate Function | Window Agg Func with Watermark
------------- | -------------------------------|-----------
Append  | Records added are aggregated and published, cannot undergo more changes (per Append Def), but records falling within Window need to be updated (per Window policy, so **CONFLICT** exists, hence **N/A** | if Window gets expired based on Watermark, then those aggregated record will not undergo any change, this records will be published. So **OUTPUT** written to Sink will not be repeated and can be **CLEARED** from State Store, hence very **EFFECTIVE**, **Downsize** is have to wait until the window/record contained gets expired or Watermark duration has occured. **APPEND mode will suppress the output of the window aggregate until they cross the watermark boundary**
Update  | Records added can be updated, since it the previous state (Aggregated result) is stored, so records falling within Window can be updated, this is **EFFICIENT and EFFECTIVE**. Every Window update is published to the sink | **Same as Window Aggregate Function except for the window expires on crossing the watermark boundary**
Complete  | Records added are added to the all the previous batch records, Aggregation is performed on the complete data since start of the application (checkpointLocation is not required). this is **NOT EFFICIENT**. All Window are published to sink. | **Same as Window Aggregate Function except for the window expires on crossing the watermark boundary**
