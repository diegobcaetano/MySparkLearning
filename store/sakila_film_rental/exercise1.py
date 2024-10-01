from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f

from store.sakila_film_rental.load_dfs import *

spark = (SparkSession
         .builder
         .master("local[3]")
         .appName("SakilaFilmRental")
         .getOrCreate())

film_df = load_films(spark)
film_df = film_df.withColumn("special_features_array", f.split(f.trim(f.col("special_features")), ","))
features = ["Trailers", "Commentaries", "Deleted Scenes", "Behind the Scenes"]
for feature in features:
    feature_column_name = feature.lower().replace(" ", "_")
    film_df = film_df.withColumn(f"feature_{feature_column_name}",
                                 f.expr(f"array_contains(special_features_array, '{feature}')").cast("int"))

film_df = (film_df
           .drop('special_features_array')
           .drop('special_features')
           .drop('description')
           .withColumn('rating',
                       f.when(f.col("rating") == "NC-17", 'ADULTS_ONLY')
                       .when(f.col("rating") == "R", 'ADULTS_ONLY')
                       .when(f.col("rating") == "PG-13", 'PARENTAL_GUIDANCE')
                       .when(f.col("rating") == "PG", 'PARENTAL_GUIDANCE')
                       .when(f.col("rating") == "G", 'GENERAL')))

print(f"Number of records: {film_df.count()}")
film_df.show()

customer_df = load_customers(spark)
print(f"Number of records: {customer_df.count()}")
customer_df.show()

rental_df = load_rental(spark)
print(f"Number of records: {rental_df.count()}")
rental_df.show()

# Q. How many movies a have for each category?

films_by_categories = (film_df
                       .groupBy('category')
                       .count()
                       .orderBy(f.desc('count'))
                       .show())

# Q. Movies with certain features rent more than others? ( What is the impact of the feature X in the renting rate)

# Q. Movies with bigger rental_duration rent more than others? ( What is the impact of the rental_rate in the renting rate)

# Q. The numbers of rentals for each category for customers from Argentina

(rental_df
 .alias('rentals')
 .join(customer_df.alias('customers'), customer_df['customer_id'] == rental_df['customer_id'])
 .join(film_df.alias('films'), film_df['film_id'] == rental_df['film_id'])
 .where(f.expr('customers.country = "Argentina"'))
 .select('customers.country', 'films.category')
 .groupBy('films.category')
 .count()
 .orderBy(f.desc('count'))
 .show())

# Q. Customers who have not returned the films yet

(rental_df
 .alias('rentals')
 .join(customer_df.alias('customers'), customer_df['customer_id'] == rental_df['customer_id'])
 .join(film_df.alias('films'), film_df['film_id'] == rental_df['film_id'])
 .select("customers.first_name", "customers.email", "films.title", "rentals.rental_date")
 .where(f.expr("return_date is null"))
 .show())

# V. Spending accumulated of a customer by month (customer_id: 148 OR 526 OR 144 OR 236

customer_year_window = Window.partitionBy("rental_year").orderBy("rental_month")

customer_rentals_and_value_df = (rental_df
                                 .alias('rentals')
                                 .join(film_df.alias('films'), film_df['film_id'] == rental_df['film_id'])
                                 .select('rentals.customer_id', 'rentals.rental_date', 'films.film_id',
                                         'films.rental_rate')
                                 .withColumn('rental_month', f.month('rental_date'))
                                 .withColumn('rental_year', f.year('rental_date')))

(customer_rentals_and_value_df
 .filter(f.col('customer_id') == 148)
 .groupBy('rental_year', 'rental_month')
 .agg(f.sum('rental_rate').alias('expending'))
 .withColumn('expending_accumulated', f.sum('expending').over(customer_year_window))
 .orderBy('rental_year', 'rental_month')
 .show())

# Q. Top 10 rented movies of each month

films_rental_by_month = (customer_rentals_and_value_df
                         .groupBy('film_id', 'rental_year', 'rental_month')
                         .count()
                         .orderBy('rental_year', 'rental_month'))

# Creating a window to order the rental count within a year and month
ym_window_order_by_count = Window.partitionBy("rental_year", "rental_month").orderBy(f.desc("count"))

top10_movies = (films_rental_by_month.withColumn("rank", f.row_number().over(ym_window_order_by_count))
                .filter(f.col("rank") <= 10)
                .select("film_id", "rental_year", "rental_month", "count"))

# Q. The films which are in top 10 of the month more than once
top10_movies.groupBy('film_id').count().filter(f.expr('count > 1')).orderBy(f.desc('count'))
