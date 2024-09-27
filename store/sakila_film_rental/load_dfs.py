from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

DB_HOST = "localhost"
DB_PORT = 3306
DB_USER = "sakila"
DB_PASS = "p_ssW0rd"  # you're not seeing it
DB_NAME = "sakila"  # close your eyes

jdbcUrl = f"jdbc:mysql://{DB_HOST}:{DB_PORT}/{DB_NAME}?user={DB_USER}&password={DB_PASS}"


def load_films(spark: SparkSession) -> DataFrame:
    query = """
    SELECT f.*, c.name as category 
    FROM film f 
    LEFT JOIN film_category fc ON f.film_id = fc.film_id 
    INNER JOIN category c ON fc.category_id = c.category_id
    """

    return (spark
            .read
            .format("jdbc")
            .option("query", query)
            .option("url", jdbcUrl)
            .option("driver", "com.mysql.jdbc.Driver")
            .option("inferSchema", "true")
            .load())


def load_customers(spark: SparkSession) -> DataFrame:
    customer_query = """
    SELECT c.*, a.address, a.district, a.postal_code, a.phone, ci.city_id, ci.city, co.country_id, co.country 
    FROM customer c
    INNER JOIN address a ON c.address_id = a.address_id
    INNER JOIN city ci ON a.city_id = ci.city_id
    INNER JOIN country co ON ci.country_id = co.country_id
    """

    return (spark
            .read
            .format("jdbc")
            .option("query", customer_query)
            .option("url", jdbcUrl)
            .option("driver", "com.mysql.jdbc.Driver")
            .option("inferSchema", "true")
            .load())


def load_rental(spark: SparkSession) -> DataFrame:
    rental_query = """
    SELECT r.*, i.film_id, i.store_id FROM sakila.rental r
    INNER JOIN inventory i ON r.inventory_id = i.inventory_id
    """

    return (spark
            .read
            .format("jdbc")
            .option("query", rental_query)
            .option("url", jdbcUrl)
            .option("driver", "com.mysql.jdbc.Driver")
            .option("inferSchema", "true")
            .load())

