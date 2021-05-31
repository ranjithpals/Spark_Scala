**UseCase02 - Apply Date Functions and perform Aggregations**
- Extract the Whole Month from the date value - date_format(<date>, "MMMM")
- Extract the three characters of month from the date value - date_format(<date>, "MMM")
- Extract the number of the month - cast(date_format(<date>, "M")) as Int)
- To display only the first record's column value, this is the aggregation used to display the first value of the column
- first(column) --> Display the first value of the column
