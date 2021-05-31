**UseCase02**
- **Apply Date Functions and perform Aggregations**
  - Extract the Whole Month from the date value - date_format(<date>, "MMMM")
  - Extract the three characters of month from the date value - date_format(<date>, "MMM")
  - Extract the number of the month - cast(date_format(<date>, "M")) as Int)
  - To display only the first record's column value, this is the aggregation used to display the first value of the column
  - first(column) --> Display the first value of the column
- **Pivot Table**
  - SQL query should just have SELECT and FROM statements
  - Group By and Pivot should be applied only to a DataFrame
  - .groupBy(<column>).pivot(<column>).count() {aggregation}
+-----+-------+--------+-----+-----+-----+-----+-----+------+---------+-------+--------+--------+
|level|January|Febraury|March|April|  May| June| July|August|September|October|November|December|
+-----+-------+--------+-----+-----+-----+-----+-----+------+---------+-------+--------+--------+
| INFO|  29119|    null|29095|29302|28900|29143|29300| 28993|    29038|  29018|   23301|   28874|
|ERROR|   4054|    null| 4122| 4107| 4086| 4059| 3976|  3987|     4161|   4040|    3389|    4106|
| WARN|   8217|    null| 8165| 8277| 8403| 8191| 8222|  8381|     8352|   8226|    6616|    8328|
|FATAL|     94|    null|   70|   83|   60|   78|   98|    80|       81|     92|   16797|      94|
|DEBUG|  41961|    null|41652|41869|41785|41774|42085| 42147|    41433|  41936|   33366|   41749|
+-----+-------+--------+-----+-----+-----+-----+-----+------+---------+-------+--------+--------+
