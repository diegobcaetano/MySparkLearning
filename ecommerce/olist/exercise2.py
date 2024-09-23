from ecommerce.olist.dfs import *
from pyspark.sql.functions import col, count, avg, desc, round, expr, sum, when, unix_timestamp, month, year

spark = (SparkSession
         .builder
         .master("local[3]")
         .appName("OrderLearning")
         .getOrCreate())

product_df = load_products(spark)
order_items_df = load_order_items(spark)
order_df = load_orders(spark)
customer_df = load_customers(spark)

# 6. Total Sales per Product Category

product_sold_with_category_df = (order_items_df
                                 .alias('order_items')
                                 .join(product_df.alias('product'),
                                       product_df['product_id'] == order_items_df['product_id'])
                                 .select(col('order_items.order_item_id').alias('units_sold'),
                                         col('product.product_id'),
                                         col('product.product_category_name').alias('category'))
                                 .sort(desc('units_sold')))

sales_per_category_df = (product_sold_with_category_df
                         .groupBy('category', 'product_id')
                         .agg(sum('units_sold').alias('total'))
                         .select('category', 'total')
                         .groupBy('category')
                         .agg(sum('total').alias('total'))
                         .sort(desc('total')))

sales_per_category_df.cache()

# 7. Categorize customers by expending

customer_avg_expending_df = load_customers_avg_expending(spark)

customer_categorized_df = (customer_avg_expending_df
                           .alias('cus_avg_exp')
                           .withColumn('category',
                                       when(expr('cus_avg_exp.total_avg > 250'), 'prime')
                                       .when(expr('cus_avg_exp.total_avg > 100 AND cus_avg_exp.total_avg < 250'),
                                             'normal')
                                       .otherwise('basic')))

customer_categorized_df.cache()

# 8. State distribution of the customers who bought something

customers_state_distribution_df = (order_items_df
                                   .alias('order_items')
                                   .join(order_df.alias('order'), expr('order_items.order_id == order.order_id'),
                                         'inner')
                                   .select(col('order.customer_id').alias('order_customer_id'))
                                   .distinct()
                                   .join(customer_df.alias('customer'),
                                         expr('customer.customer_id == order_customer_id'))
                                   .select(col('customer.customer_state').alias('state'),
                                           col('customer.customer_id').alias('customer_id'))
                                   .groupBy('state')
                                   .count()
                                   .sort(desc('count')))

customers_state_distribution_df.cache()

# 9. Customers who never bought something

customers_never_bought_df = (customer_df
                             .join(order_df, customer_df['customer_id'] == order_df['customer_id'], 'left')
                             .where(expr('order_id is null'))
                             .select(customer_df['*']))

customers_never_bought_df.cache()

# 10. Delivery time average by state

delivery_time_avg_by_state_df = (order_df
                                 .alias('orders')
                                 .where(expr('order_status == "delivered"'))
                                 .withColumn('delivery_time_in_hours',
                                             (unix_timestamp(col('order_delivered_customer_date')) -
                                              unix_timestamp(col('order_delivered_carrier_date'))) / 3600)
                                 .withColumn('delivery_time_in_hours', round('delivery_time_in_hours', 2))
                                 .join(customer_df.alias('customers'),
                                       expr('customers.customer_id == orders.customer_id'), 'inner')
                                 .select('delivery_time_in_hours', 'customers.customer_state')
                                 .groupBy('customer_state')
                                 .agg(round(avg('delivery_time_in_hours'), 2).alias('delivery_time_avg'))
                                 .sort(desc('delivery_time_avg')))

delivery_time_avg_by_state_df.cache()

# 11. Rate of order cancellation by month

cancellation_rate_df = (order_df
                        .alias('orders')
                        .withColumn('orders_month', month('orders.order_purchase_timestamp'))
                        .withColumn('is_cancelled', when(expr('orders.order_status == "canceled"'), 1).otherwise(0))
                        .groupBy('orders_month')
                        .agg(sum('is_cancelled').alias('total_cancelled'),
                             count('*').alias('total_orders_month'))
                        .withColumn('rate_cancelled_month', expr('(total_cancelled / total_orders_month) * 100'))
                        .select('orders_month', 'total_orders_month', 'total_cancelled', 'rate_cancelled_month'))

cancellation_rate_df.cache()

# 12. Does the order was delivered on estimated time?

delivered_on_time_df = (order_df
                        .alias('orders')
                        .where(expr('orders.order_status == "delivered" and order_delivered_customer_date is not null'))
                        .withColumn('month', month('orders.order_purchase_timestamp'))
                        .withColumn('year', year('orders.order_purchase_timestamp'))
                        .withColumn('not_on_time',
                                    when(expr(
                                        'orders.order_delivered_customer_date > orders.order_estimated_delivery_date'),
                                         1).otherwise(0))
                        .groupBy('year', 'month')
                        .agg(sum('not_on_time').alias('num_late_deliveries'),
                             count('*').alias('total_orders'))
                        .withColumn('percentage', round(expr('(num_late_deliveries / total_orders) * 100'), 2))
                        .orderBy(desc('year'), desc('month'))
                        .show())

delivered_on_time_df.show()
