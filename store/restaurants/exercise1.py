from pyspark.sql import SparkSession
from pyspark.sql import functions as f

BASE_PATH_DF = "../../data/restaurants"

spark = (SparkSession
         .builder
         .appName("RestaurantUnion")
         .master("local[3]")
         .getOrCreate())

restaurants_wake_df = (spark
                       .read
                       .format("csv")
                       .option("header", "true")
                       .load(f"{BASE_PATH_DF}/Restaurants_in_Wake_County.csv"))

print(f"Number of restaurants in Wake: {restaurants_wake_df.count()}")
print(f"Number of partitions in this dataset: {restaurants_wake_df.rdd.getNumPartitions()}")

restaurants_wake_cleaned_df = (restaurants_wake_df
                               .drop('OBJECTID')
                               .drop('PERMITID')
                               .drop('GEOCODESTATUS')
                               .drop('SHAPE')
                               .withColumn('county', f.lit('Wake'))
                               .withColumnRenamed('NAME', 'name')
                               .withColumnRenamed('HSISID', 'datasetId')
                               .withColumnRenamed('ADDRESS1', 'address1')
                               .withColumnRenamed('ADDRESS2', 'address2')
                               .withColumnRenamed('CITY', 'city')
                               .withColumnRenamed('STATE', 'state')
                               .withColumnRenamed('POSTALCODE', 'zip')
                               .withColumnRenamed('PHONENUMBER', 'tel')
                               .withColumnRenamed('RESTAURANTOPENDATE', 'dateStart')
                               .withColumnRenamed('FACILITYTYPE', 'type')
                               .withColumnRenamed('X', 'geoX')
                               .withColumnRenamed('Y', 'geoY')
                               .withColumn('dateEnd', f.lit(None))
                               .withColumn('id', f.concat(f.col('state'),
                                                          f.lit('_'),
                                                          f.col('county'),
                                                          f.lit('_'),
                                                          f.col('datasetId'))))

restaurants_durham_df = (spark
                         .read
                         .format("json")
                         .load(f"{BASE_PATH_DF}/Restaurants_in_Durham_County_NC.json"))

print(f"Number of restaurants in Durham: {restaurants_durham_df.count()}")
print(f"Number of partitions in this dataset: {restaurants_durham_df.rdd.getNumPartitions()}")

restaurants_durham_cleaned_df = (restaurants_durham_df
                                 .withColumn('datasetId', f.col('fields.id'))
                                 .withColumn('name', f.col('fields.premise_name'))
                                 .withColumn('address1', f.col('fields.premise_address1'))
                                 .withColumn('address2', f.col('fields.premise_address2'))
                                 .withColumn('city', f.col('fields.premise_city'))
                                 .withColumn('state', f.col('fields.premise_state'))
                                 .withColumn('zip', f.col('fields.premise_zip'))
                                 .withColumn('tel', f.col('fields.premise_phone'))
                                 .withColumn('dateStart', f.col('fields.opening_date'))
                                 .withColumn('dateEnd', f.col('fields.closing_date'))
                                 .withColumn('type', f.split(f.col('fields.type_description'), " - ").getItem(1))
                                 .withColumn('geoX', f.col('fields.geolocation').getItem(0))
                                 .withColumn('geoY', f.col('fields.geolocation').getItem(1))
                                 .withColumn('county', f.lit('Durham'))
                                 .withColumn('id', f.concat(f.col('state'),
                                                            f.lit('_'),
                                                            f.col('county'),
                                                            f.lit('_'),
                                                            f.col('datasetId')))
                                 .drop('fields', 'geometry', 'record_timestamp', 'recordid'))

all_restaurants_df = restaurants_wake_cleaned_df.unionByName(restaurants_durham_cleaned_df)
all_restaurants_df = (all_restaurants_df
                      .withColumn('dateStart', f.to_date('dateStart', 'yyyy-MM-dd'))
                      .withColumn('dateEnd', f.to_date('dateEnd', 'yyyy-MM-dd')))

print(f"Number of restaurants: {all_restaurants_df.count()}")
print(f"Number of partitions in this dataset: {all_restaurants_df.rdd.getNumPartitions()}")
all_restaurants_df.printSchema()
all_restaurants_df.show()