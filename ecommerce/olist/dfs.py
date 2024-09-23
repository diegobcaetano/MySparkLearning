from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import asc, col, count, avg, round, desc

BASE_PATH_ORDERS_DF = "../../data/olist_orders"


def load_products(spark: SparkSession) -> DataFrame:
    return (spark
            .read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(f"{BASE_PATH_ORDERS_DF}/olist_products_dataset.csv"))


def load_orders(spark: SparkSession) -> DataFrame:
    return (spark
            .read
            .format("csv")
            .option("inferSchema", "true")
            .option("header", "true")
            .load(f"{BASE_PATH_ORDERS_DF}/olist_orders_dataset.csv")
            .dropDuplicates(['order_id'])
            .sort(asc('order_purchase_timestamp')))


def load_order_items(spark: SparkSession) -> DataFrame:
    return (spark
            .read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(f"{BASE_PATH_ORDERS_DF}/olist_order_items_dataset.csv"))


def load_customers(spark: SparkSession) -> DataFrame:
    return (spark
            .read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(f"{BASE_PATH_ORDERS_DF}/olist_customers_dataset.csv"))


def load_geo(spark: SparkSession) -> DataFrame:
    return (spark
            .read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(f"{BASE_PATH_ORDERS_DF}/olist_geolocation_dataset.csv")
            .select(['geolocation_zip_code_prefix', 'geolocation_city', 'geolocation_state'])
            .dropDuplicates(['geolocation_zip_code_prefix']))


def load_customers_avg_expending(spark):
    order_items_df = load_order_items(spark)
    orders_df = load_orders(spark)
    order_total_value_df = (order_items_df
                            .withColumn('order_total',
                                        order_items_df.price * order_items_df.order_item_id + order_items_df.freight_value)
                            .groupBy("order_id")
                            .avg('order_total')
                            .select(col('avg(order_total)').alias('order_total_avg'), col('order_id')))

    order_total_with_customer_df = (order_total_value_df
                                    .join(orders_df, orders_df['order_id'] == order_total_value_df['order_id'],
                                          'inner')
                                    .select(order_total_value_df['*'],
                                            orders_df['customer_id'],
                                            orders_df['order_status']))
    return (order_total_with_customer_df
            .where('order_status NOT IN ("canceled", "unavailable")')
            .groupBy('customer_id')
            .agg(count('order_id').alias('purchases'),
                 round(avg('order_total_avg'), 2).alias('total_avg'))
            .select(col('customer_id'),
                    col('purchases'),
                    col('total_avg'))
            .orderBy(desc('purchases')))
