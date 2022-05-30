import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import desc
from pyspark.sql.functions import row_number
from pyspark.sql.functions import rank
from pyspark.sql.functions import dense_rank


spark = (SparkSession.builder.appName("window1").getOrCreate())


#cars = {'name':  ['800', '110', '208','Atlas','Mustang','C500', 'Prius', 'Landcruiser','Accord','C200','Corrolla'],
  #      'company': ['BMW', 'Bugatti', 'Peugeot','Volkswagen','Ford','Mercedes', 'Toyota', 'Toyota','Honda','Mercedes','Toyota'],
  #      'power': ['8000', '8000', '5400', '5000', '5000', '5000', '3200', '3000', '2000','2000', '1800']
   #     }

#coches = pd.DataFrame(cars)

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

columns = ['Name', 'Company', 'Power']

coches = spark.createDataFrame(data=data,
                               schema=columns)


windowSpec = Window.orderBy(desc("power"))

coches.withColumn("row_number",
               row_number().over(windowSpec)) \
    .withColumn("rank", rank().over(windowSpec)) \
    .withColumn("dense_rank", dense_rank().over(windowSpec)).show()