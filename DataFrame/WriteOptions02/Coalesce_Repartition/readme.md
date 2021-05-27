**DIFFERENCE BETWEEN REPARTITION AND COALESCE**

- *REPARTITION*: It can INCREASE OR DECREASE partitions
	- It is a Wide Transformation because SHUFFLING is INVOLVED.
	- It has an intention to have final partitions of exactly equal size and for this it has to go through complete shuffling.

- *COALESCE*: It can ONLY DECREASE the number of Transformation
	- It performs only Local data shuffling (Data Residing within the same Node)
	- It has an intention to reduce the shuffling and combines existing partitions on each machine to avoid a full shuffle.

- TO DECREASE THE NUMBER OF PARTITIONS COALESCE IS PREFFERED TO REPARTITION BECAUSE:
	- Coalesce tries to minimize the shuffling, the shuffling is restricted to the same NODE.
	- The Size of the new Partitions are RESTRICTED only to partitions within the same NODE.
	- Repartition involves the shuffling across NODES
	- The size of the new Partitions are NOT restricted to partitions within the SAME NODE.

- coalesce uses existing partitions to minimize the amount of data that's shuffled. 
- repartition creates new partitions and does a full shuffle. 
- coalesce results in partitions with different amounts of data (sometimes partitions that have much different sizes) and 
- repartition results in roughly equal sized partitions.

- **Is coalesce or repartition faster?**

coalesce may run faster than repartition, but unequal sized partitions are generally slower to work with than equal sized partitions. You'll usually need to repartition datasets after filtering a large data set. I've found repartition to be faster overall because Spark is built to work with equal sized partitions.

N.B. I've curiously observed that repartition can increase the size of data on disk. Make sure to run tests when you're using repartition / coalesce on large datasets.

Read this blog post if you'd like even more details.

When you'll use coalesce & repartition in practice

See this question on how to use coalesce & repartition to write out a DataFrame to a single file
It's critical to repartition after running filtering queries. The number of partitions does not change after filtering, so if you don't repartition, you'll have way too many memory partitions (the more the filter reduces the dataset size, the bigger the problem). Watch out for the empty partition problem.
partitionBy is used to write out data in partitions on disk. You'll need to use repartition / coalesce to partition your data in memory properly before using partitionBy.

- https://stackoverflow.com/questions/31610971/spark-repartition-vs-coalesce

- https://blog.knoldus.com/apache-spark-repartitioning-v-s-coalesce/
