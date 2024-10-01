from pyspark.sql import SparkSession
from pyspark.sql import functions as f

BASEPATH = "../data/government/census"

spark = (SparkSession
         .builder
         .master("local[3]")
         .appName("CensusTraining")
         .getOrCreate())

df = (spark
      .read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(f"{BASEPATH}/PEP_2017_PEPANNRES.csv"))

intermediate_df = (df.
                   drop('GEO.id')
                   .withColumnRenamed('GEO.display-label', 'label')
                   .withColumnRenamed('GEO.id2', 'id')
                   .withColumnRenamed('rescen42010', 'real2010')
                   .drop('resbase42010')
                   .withColumnRenamed('respop72010', 'est2010')
                   .withColumnRenamed('respop72011', 'est2011')
                   .withColumnRenamed('respop72012', 'est2012')
                   .withColumnRenamed('respop72013', 'est2013')
                   .withColumnRenamed('respop72014', 'est2014')
                   .withColumnRenamed('respop72015', 'est2015')
                   .withColumnRenamed('respop72016', 'est2016')
                   .withColumnRenamed('respop72017', 'est2017'))

intermediate_df = (intermediate_df
                   .withColumn('countyState', f.split('label', ','))
                   .withColumn('stateId', f.expr('int(id/1000)'))
                   .withColumn('countyId', f.expr('id%1000'))
                   .withColumn('state', f.col('countyState').getItem(1))
                   .withColumn('county', f.col('countyState').getItem(0))
                   .drop('countyState'))

intermediate_df.sample(.01).show(10, False)