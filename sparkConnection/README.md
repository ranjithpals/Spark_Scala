- Context, Session are essentially ENTRY POINTS FOR SPARK APPLICATIONS (typically to cluster, but same applies to Standalone mode as well) 
- Every APPLICATION needs an ENTRY POINT to communicate with data sources and perform operations such as reading and writing.

ENTRY POINTS
- These modes were introduced in Spark 1.x
- Spark Context, SQL Context, Hive Context 
- New mode was introduced in Spark 2.x

Spark Session
- SparkSession has been introduced that essentially combined all functionalities available in the three aforementioned contexts. 
- Note that all contexts are still available even in newest Spark releases, mostly for backward compatibility purposes.

From <https://towardsdatascience.com/sparksession-vs-sparkcontext-vs-sqlcontext-vs-hivecontext-741d50c9486a> 

**Analogy:**
Assume we are ENTERING to a Theme Park with multiple rides with each RIDE needing its own GEAR( Context or Session) to be worn in order to ENTER the RIDE (perform Operations on HIVE, SPARK, SQL) OR wear ONE UNIVERSAL GEAR which gives access to ALL RIDES (SPARK SESSION)

- SparkContext:
The SparkContext is used by the Driver Process of the Spark Application in order to establish a communication with the cluster and the resource managers in order to coordinate and execute jobs. 
SparkContext also enables the access to the other two contexts, namely SQLContext and HiveContext (more on these entry points later on).

- SQLContext:
SQLContext is the entry point to SparkSQL which is a Spark module for structured data processing. Once SQLContext is initialised, the user can then use it in order to perform various “sql-like” operations over Datasets and DataFrame.
In order to create a SQLContext, you first need to instantiate a SparkContext as shown below:

- HiveContext:
If your Spark Application needs to communicate with Hive and you are using Spark < 2.0 then you will probably need a HiveContext if . For Spark 1.5+, HiveContext also offers support for window functions.

- SparkSession:
Spark 2.0 introduced a new entry point called SparkSession that essentially replaced both SQLContext and HiveContext. 
Additionally, it gives to developers immediate access to SparkContext. In order to create a SparkSession with Hive support, all you have to do is in the above code.


- HAVING MULTIPLE SESSIONS AND USING SAME CONTEXT UNDERNEATH
- HAVING MULTIPLE CONTEXTS
- ADVANTAGES AND DISADVANTAGES

https://medium.com/@achilleus/spark-session-10d0d66d1d24
