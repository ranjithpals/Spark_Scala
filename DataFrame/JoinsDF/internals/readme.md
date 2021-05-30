**Internals of DataFrame Joins**

- Let's assume we have data to be processed in two executors.

- Executor 1 - Node 1
> Customer Data
```
> 3, Ann, Smith, XXXXXXX, XXXX, 3422 Blue Pioneer Bend, Cagaus, PR, 00725
```
> Orders Data
```
> 15192, 2013-10-29 00:00:00.0, 2, PENDING_PAYMENT
> 33865, 2014-02-18 00:00:00.0, 2, COMPLETE
```
- Executor 2 - Node 2
> Customer Data
```
> 2, Mary, Bannett, XXXXXXX, XXXX, 9256 Noble Embers Ridge, Littelton, CO, 80126
```
> Orders Data
```
> 35158, 2014-02-26 00:00:00.0, 3, COMPLETE
> 15192, 2013-10-29 00:00:00.0, 2, PENDING_PAYMENT
```
- Upon Join Operation on customer_id
```
> (2, {15192, 2013-10-29 00:00:00.0, 2, PENDING_PAYMENT})
> (2, {33865, 2014-02-18 00:00:00.0, 2, COMPLETE})

> (3, {3, Ann, Smith, XXXXXXX, XXXX, 3422 Blue Pioneer Bend, Cagaus, PR, 00725})
```
- Executor 3 - Node 3
```
> 15192, 2013-10-29 00:00:00.0, 2, PENDING_PAYMENT
> 2, Mary, Bannett, XXXXXXX, XXXX, 9256 Noble Embers Ridge, Littelton, CO, 80126
```

**Steps Involved**
1. read the data and convert it to Key-Value pair 
2. Write the output into an exchange (exchange is a buffer in the executor)
3. From this exchange, Spark Framework can read it and perform the shuffle.
4. All the records with same Key (customer_id) are sent to the reducer exchange - SHUFFLE.
5. SORT - The records are sorted after the shuffle, then MERGED and JOIN is performed.
![image](https://user-images.githubusercontent.com/39640906/120114114-97e4e300-c14b-11eb-96dd-9e44f2758478.png)

**INNER JOIN - AUTOMATICALLY FACILITATES AUTO-BROADCASTING**
![image](https://user-images.githubusercontent.com/39640906/120092258-82d26a80-c0df-11eb-8eac-bd621161f896.png)

**TO RESTRICT AUTOMATIC BROADCASTING DURING JOIN OPERATIONS**
1. Set the Spark sql parameter 'spark.sql.autoBroadcastJoinThreshold = -1'
![image](https://user-images.githubusercontent.com/39640906/120092298-d80e7c00-c0df-11eb-8100-164a5ca9f0b1.png)



