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
+---+--------+-----+
|_c0|     _c1|  _c2|
+---+--------+-----+
|  1| 'Apple'|10.99|
|  2|'Orange'|11.99|
|  3|'Banana'|12.99|
+---+--------+-----+
```
- fruits.txt
```
1,'Apple',10.99
2,'Orange',11.99
3,'Banana',12.99
```
