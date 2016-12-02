

```python
# Import and create a new SQLContext 
from pyspark.sql import SQLContext, Row
sqlContext = SQLContext(sc)
```


```python
# Read the country CSV file into an RDD.
country_lines = sc.textFile('file:///home/cloudera/Downloads/big-data-3/final-project/country-list.csv')
```


```python
# Convert each line into a pair of words
word_pairs = country_lines.map( lambda l: l.split(","))
type(word_pairs)
```




    pyspark.rdd.PipelinedRDD




```python
# Convert each pair of words into a tuple
country_tuples = word_pairs.map(lambda p: Row(name=p[0], zip=(p[1])))

```


```python
# Create the DataFrame, look at schema and contents
countryDF = sqlContext.createDataFrame(country_tuples, ["country", "code"])
countryDF.printSchema()
countryDF.take(3)
```

    root
     |-- country: string (nullable = true)
     |-- code: string (nullable = true)
    





    [Row(country='Afghanistan', code=' AFG'),
     Row(country='Albania', code=' ALB'),
     Row(country='Algeria', code=' ALG')]




```python
# Read tweets CSV file into RDD of lines
tweet_lines = sc.textFile('file:///home/cloudera/Downloads/big-data-3/mongodb/tweet_data.csv')
```


```python
# Clean the data: some tweets are empty. Remove the empty tweets using filter() 
```


```python
# Perform WordCount on the cleaned tweet texts. (note: this is several lines.)
tweet_lines.count()
words = tweet_lines.flatMap( lambda line : line.split(" "))
tuples = words.map(lambda word : (word, 1))
counts = tuples.reduceByKey(lambda a, b: (a + b))
```


```python
# Create the DataFrame of tweet word counts
tweetDF = sqlContext.createDataFrame(counts, ["word", "count"])
tweetDF.printSchema()
tweetDF.take(3)
```

    root
     |-- word: string (nullable = true)
     |-- count: long (nullable = true)
    





    [Row(word='', count=2772),
     Row(word='{"_id":{"$oid":"57966dc7c38159201ca7f677"},"tweet_text":"RT', count=1),
     Row(word='mobile', count=1)]




```python
# Join the country and tweet data frames (on the appropriate column)
df = countryDF.join(tweetDF, countryDF.country == tweetDF.word)
```


```python
# Question 1: number of distinct countries mentioned
df.count()
```




    42




```python
# Question 2: number of countries mentioned in tweets. need to apply sum to df column "count"
from pyspark.sql.functions import sum
df.groupBy().sum("count").collect()
```




    [Row(sum(count)=349)]




```python
# Table 1: top three countries and their counts.
from pyspark.sql.functions import desc
df.sort(desc("count")).take(5)
```




    [Row(country='Nigeria', code=' NGA', word='Nigeria', count=48),
     Row(country='France', code=' FRA', word='France', count=38),
     Row(country='Slovakia', code=' SVK', word='Slovakia', count=30),
     Row(country='Norway', code=' NOR', word='Norway', count=28),
     Row(country='England', code=' ENG', word='England', count=25)]




```python
# Table 2: counts for Wales, Iceland, and Japan.
df.where(df['country'].isin({'Wales', 'Kenya', 'Netherlands'})).show()
```

    +-----------+----+-----------+-----+
    |    country|code|       word|count|
    +-----------+----+-----------+-----+
    |      Wales| WAL|      Wales|   18|
    |Netherlands| NED|Netherlands|   13|
    |      Kenya| KEN|      Kenya|    3|
    +-----------+----+-----------+-----+
    



```python
df.agg({"count" : "avg"}).show()
```

    +----------------+
    |      avg(count)|
    +----------------+
    |8.30952380952381|
    +----------------+
    

