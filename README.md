# pySpark

```
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

courses = [(1,'Apple',10.99),(2,'Orange',11.99) ,(3,'Banana',12.99)]
df = spark.createDataFrame(courses, ['Id', 'Name', 'Price'])
df.show()
```
```
+---+------+-----+
| Id|  Name|Price|
+---+------+-----+
|  1| Apple|10.99|
|  2|Orange|11.99|
|  3|Banana|12.99|
+---+------+-----+
```
```
from pyspark.sql.functions import desc
from pyspark.sql.functions import col

sorteddf=df.sort(col("Price").desc())
sorteddf.show()
```
```
+---+------+-----+
| Id|  Name|Price|
+---+------+-----+
|  3|Banana|12.99|
|  2|Orange|11.99|
|  1| Apple|10.99|
+---+------+-----+
```
```
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

df = spark.read.option('header','false') \
               .option('delimiter',',') \
               .csv('fruits.txt')
df.show()
```
```
+---+------+-----+
|_c0|   _c1|  _c2|
+---+------+-----+
|  1| Apple|10.99|
|  2|Orange|11.99|
|  3|Banana|12.99|
+---+------+-----+
```
- fruits.txt
```
1,Apple,10.99
2,Orange,11.99
3,Banana,12.99
```
```
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

df = spark.read.option('header','true') \
               .option('delimiter',',') \
               .csv('fruits.txt')
df.show()
```
```
+---+------+-----+-----------+
| id|  name|price|    country|
+---+------+-----+-----------+
|  1| Apple|10.99|      Japan|
|  2|Orange|11.99|      Japan|
|  3|Banana|12.99|      Japan|
|  4| Apple|10.99|        USA|
|  5|Orange|11.99|        USA|
|  6|Banana|12.99|     Taiwan|
|  7|Orange|11.99|     Israel|
|  8|Banana|12.99|Philippines|
+---+------+-----+-----------+
```
- fruits.txt
```
id,name,price,country
1,Apple,10.99,Japan
2,Orange,11.99,Japan
3,Banana,12.99,Japan
4,Apple,10.99,USA
5,Orange,11.99,USA
6,Banana,12.99,Taiwan
7,Orange,11.99,Israel
8,Banana,12.99,Philippines
```
```
newdf=df.groupby(df['country']) \
      .count().sort(desc("count"))

newdf.show()
```
```
+-----------+-----+
|    country|count|
+-----------+-----+
|      Japan|    3|
|        USA|    2|
|Philippines|    1|
|     Taiwan|    1|
|     Israel|    1|
+-----------+-----+
```
```
df.createOrReplaceTempView("tempview")
result=spark.sql("SELECT name, count(name) from tempview Group By name")
result.show()
```
```
+------+-----------+
|  name|count(name)|
+------+-----------+
|Orange|          3|
|Banana|          3|
| Apple|          2|
+------+-----------+
```
