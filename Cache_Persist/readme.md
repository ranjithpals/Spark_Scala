**EVERY ACTION IS CONSIDERED A JOB in SPARK

**Both Cache and Persist perform the same function**

	- USED to RE-USE the results of an EXISTING RDD.
	- SPEEDS up the application which ACCESSES THE SAME RDD multiple times.
	- RDD which is NOT Cached is evaluated/calculated again each time an ACTION is invoked.
	- CACHE is STRICTLY IN-MEMORY
	- PERSIST comes with various STORAGE LEVELS

**RDD NOT CACHED**

	- The result of map function of Stage 1 is an RDD
	- The RDD is further transformed in the next stages (like Stage 3, Stage 4)
	- When the ACTION is called the first time, the whole JOB (multiple stages) are executed
		○ ACTION is used to calculate the RESULT or Intermediate RESULT
	- In the Scenario that the RDD from map function of Stage 1 is required to perform another ACTION
		○  Like JOIN with another RDD and perform another RESULTANT DataFrame or RESULT (variable)
	- ALL the STAGES starting from Stage 0 is EXECUTED to reach to the point of RDD (map function of Stage 1)
	- Let's assume the this RDD (data frame) is a lookup table which is used in MULTIPLE JOINS
	- This means the EVERY TIME AN ACTION is Performed (Data Frame created from a JOIN), ALL the STAGES are executed, which is uses up a LOT of CPU and MEMORY

**RDD IS CACHED**
	- Cached RDD is NOT executed from the Initial Stage (In reality SPARK does not execute from the Initial 
	- AVOIDS READING FROM DISK when used in FURTHER operations

**SCENARIOS WHERE IT MAKES A DIFFERENCE**
	- RDD is used in multiple operations since it is created
	- RDD is used for multiple iteration of calculations for ML related use cases
	- RDD has multiple stages with many number of Maps (used for transformations) - only CURRENT Stage is invoked when called multiple times
	- Job STAGE results are HUGE due to LARGE Dataset used as Input for the STAGE

**PERSIST**
	- Persist() same as Cache() - Data is cached in Memory. NON Serialized Format
	- Persist(StorageLevel.DISK_ONLY) - Data is cached on DISK in Serialized Format in form of bytes and takes less bytes.
	- Persist(MEMORY_AND_DISK) - Data is cached in Memory. If enough memory is NOT available, evicted blocks from memory are serialized to DISK
		○ This mode is used when re-valuation is expensive and memory is sparse.
	- Persist(OFF_HEAP) - Blocks are cached off heap. Outside the JVM, grabbing a piece of memory outside the executor
		○ Problem with storing objects in JVM, is it uses GARBAGE COLLECTION to freeing-up, which is a time-taking process.
		○ OFF_HEAP is UNSAFE as we have to deal with raw-memory outside the JVM.
		
**BLOCK EVICTION**
	- consider a scenario where the block partitions are so skew that they will quickly fill up the storage memory used for caching.
	- when the storage memory becomes full, an eviction will be used to make up for new blocks which is based on LRU - Least Recently Used
	- Serialization: increases processing cost but reduces memory foot print (Save to DISK, more processing, less memory usage)
	- Non-Serialization: decreases processing cost but increases memory foot print (Stores in memory, less processing, more memory usage)

**NOT CACHED**
![image](https://user-images.githubusercontent.com/39640906/117525209-27550700-af8f-11eb-848a-8afb440f5230.png)

![image](https://user-images.githubusercontent.com/39640906/117525218-320f9c00-af8f-11eb-9748-3d61b0ef74b1.png)

**CACHED**
In SPARK UI cache is represented as GREEN DOT.
![image](https://user-images.githubusercontent.com/39640906/117525226-3b990400-af8f-11eb-9fc5-98f3a0fd1853.png)
