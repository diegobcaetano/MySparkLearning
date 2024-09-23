from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, count, avg, desc, round, max, sum, row_number

from ecommerce.olist.dfs import load_orders, load_order_items, load_products, load_geo, load_customers

spark = (SparkSession
         .builder
         .master("local[3]")
         .appName("OrderLearning")
         .getOrCreate())

orders_df = load_orders(spark)
orders_df.cache()
orders_df.groupBy('order_status').count().show()

order_items_df = load_order_items(spark)
order_items_df.cache()
# order_items_df.printSchema()
# order_items_df.show()

products_df = load_products(spark)
customers_df = load_customers(spark)

customers_df.cache()

geo_grouped_df = load_geo(spark)
geo_grouped_df.cache()

# 1. Customers who bought more than once

order_total_value_df = (order_items_df
                        .withColumn('order_total',
                                    order_items_df.price * order_items_df.order_item_id + order_items_df.freight_value)
                        .groupBy("order_id")
                        .avg('order_total')
                        .select(col('avg(order_total)').alias('order_total_avg'),
                                col('order_id')))

order_total_with_customer_df = (order_total_value_df
                                .join(orders_df, orders_df['order_id'] == order_total_value_df['order_id'], 'inner')
                                .select(order_total_value_df['*'],
                                        orders_df['customer_id'],
                                        orders_df['order_status']))

customers_with_more_purchases_df = (orders_df
                                    .groupBy('customer_id')
                                    .agg(count('order_id').alias('purchases'))
                                    .filter(col('purchases') > 1))

# 2. Customer average order value

question1_df = (order_total_with_customer_df
                .where('order_status NOT IN ("canceled", "unavailable")')
                .groupBy('customer_id')
                .agg(count('order_id').alias('purchases'),
                     round(avg('order_total_avg'), 2).alias('total_avg'))
                .select(col('customer_id'),
                        col('purchases'),
                        col('total_avg'))
                .orderBy(desc('purchases')))

# 3. Top number of items sold in one purchase

(order_items_df
 .select(max(col('order_item_id')).alias('max_num_items_sold'))
 .show())

# 4. Products sold by state and city

products_by_city_df = (order_items_df
                       .alias('order_items')
                       .join(orders_df.alias('orders'), orders_df['order_id'] == order_items_df['order_id'], 'inner')
                       .join(customers_df.alias('customers'), customers_df['customer_id'] == orders_df['customer_id'])
                       .join(geo_grouped_df.alias('geo'),
                             customers_df['customer_zip_code_prefix'] == geo_grouped_df['geolocation_zip_code_prefix'])
                       .select(col('geo.geolocation_city').alias('city'),
                               col('order_items.product_id').alias('product'),
                               col('geo.geolocation_state').alias('state'))
                       .groupBy('state', 'city', 'product')
                       .agg(count(col('product')).alias('qtd'))
                       .sort([col('state'), col('city'), desc(col('product'))]))

products_by_city_df.cache()

# 5. The most sold product by state

windowSpec = Window.partitionBy("state").orderBy(desc("total_sales"))

most_sold_by_state = (products_by_city_df
                      .groupBy("state", "product")
                      .agg(sum("qtd").alias("total_sales"))
                      .withColumn("rank", row_number().over(windowSpec))
                      .filter(col("rank") == 1)
                      .select("state", "product", "total_sales"))

most_sold_by_state.cache()
most_sold_by_state.show()

