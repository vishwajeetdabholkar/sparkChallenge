'''
Give Jason : {"labels":[{"id":1,"name":"abs"},{"id":2,"name":"ups"}]}

Get the optput as :

+-----------+
|label_names|
+-----------+
| [abs, ups]|
+-----------+

'''

from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .appName('SparkChallenge')\
    .master('local')\
    .getOrCreate()
    
from pyspark.sql import functions as func
newJson = '{"labels":[{"id":1,"name":"abs"},{"id":2,"name":"ups"}]}'
df4 = spark.read.json(spark.sparkContext.parallelize([newJson]))
df4.show()
df4.printSchema()
#########################
df5 = df4.select(func.explode("labels").alias("data"))
df5.show()
df5.printSchema()

#########################

df6 = df5.select('data.*')
df6.show()
df6.printSchema()

#########################
df6.cache()
df7 = df6.select(func.collect_list("name").alias("label_names"))
df7.printSchema()
df7.show()

#Output:


"""
+--------------------+
|              labels|
+--------------------+
|[[1, abs], [2, ups]]|
+--------------------+

root
 |-- labels: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- id: long (nullable = true)
 |    |    |-- name: string (nullable = true)

+--------+
|    data|
+--------+
|[1, abs]|
|[2, ups]|
+--------+

root
 |-- data: struct (nullable = true)
 |    |-- id: long (nullable = true)
 |    |-- name: string (nullable = true)

+---+----+
| id|name|
+---+----+
|  1| abs|
|  2| ups|
+---+----+

root
 |-- id: long (nullable = true)
 |-- name: string (nullable = true)

root
 |-- label_names: array (nullable = true)
 |    |-- element: string (containsNull = false)

+-----------+
|label_names|
+-----------+
| [abs, ups]|
+-----------+
"""

