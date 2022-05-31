from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName("window2").getOrCreate())

data = (('800', 'BMW', 8000),
        ('110', 'Bugatti', 8000),
        ('208', 'Peugeot', 5400),
        ('Atlas', 'Volkswagen', 5000),
        ('Mustang', 'Ford', 5000),
        ('C500', 'Mercedes', 5000),
        ('Prius', 'Toyota', 3200),
        ('Landcruiser', 'Toyota', 3000),
        ('Accord', 'Honda', 2000),
        ('C200', 'Mercedes', 2000),
        ('Corrolla', 'Toyota', 1800))

columns = ['name', 'company', 'power']

coches = spark.createDataFrame(data=data,
                               schema=columns)

coches.createOrReplaceTempView("coches")

spark.sql("""
SELECT name, company, power,rank
 FROM ( 
 SELECT name, company, power, rank()
 OVER ( ORDER BY power DESC) as rank
 FROM coches
 ) 
""").show()

spark.sql("""
SELECT name, company, power,dense_rank
 FROM ( 
 SELECT name, company, power, dense_rank()
 OVER ( ORDER BY power DESC) as dense_rank
 FROM coches
 ) 
""").show()

spark.sql("""
SELECT name, company, power,row_number
 FROM ( 
 SELECT name, company, power, row_number()
 OVER ( ORDER BY power DESC) as row_number
 FROM coches
 ) 
""").show()