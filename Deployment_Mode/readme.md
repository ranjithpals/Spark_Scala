Deploy Mode Cluster Vs Client
==============================

Cluster Mode
-------------

spark-submit \
--class LoglevelGrouping \
--deploy-mode cluster \
--master yarn \
--conf spark.dynamicAllocation.enabled=false \
--num-executors 4 \
--executor-memory 3G \
Jar/wordCount.jar bigLogNew.txt

** JAR file is located in the edge node from where the job is triggered.

http://m01.itversity.com:18081/history/application_1624709891350_36460/1/jobs/

Client Mode
-------------
** If Cluster mode is NOT specified then it is Client mode by DEFAULT

spark-submit \
--class LoglevelGrouping \
--deploy-mode client \
--master yarn \
--num-executors 4 \
--executor-memory 3G \
Jar/wordCount.jar  bigLogNew.txt

Python
--------

spark-submit \
--deploy-mode cluster \
--master yarn \
--num-executors 4 \
--executor-memory 3G \
scripts/LoglevelGrouping.py bigLogNew.txt

For Python
- Jar File creation is NOT Required
- No Need to create a CLASS, specify the code within the 
```
if __name__ == '__main__':
    args = sys.argv[1]
    main(args)
```
To retrieve string arguements provided with the spark-submit command.

- Client Mode: http://m01.itversity.com:18081/history/application_1624709891350_36495/jobs/
- Cluster Mode: http://m01.itversity.com:18081/history/application_1624709891350_36497/1/jobs/

