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
