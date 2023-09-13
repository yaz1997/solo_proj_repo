# %% [markdown]
# ## Initializing spark session

# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, date_format, count, avg, stddev, mean
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('solo_proj')\
        .config('spark.driver.extraClassPath',"/opt/spark/jars/postgresql-42.6.0.jar")\
        .getOrCreate()


# %% [markdown]
# ## Encrypting usename and password
# 

# %%
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

user = os.getenv("postgres_username")
password = os.getenv("postgres_password")

# %% [markdown]
# ## Importing datasets to spark

# %%
#calendar dataframe
data_path = "./cleaned_csv/clean0_calendar_csv"  
calendar_df = spark.read.csv(data_path, header=True, inferSchema=True)

#listing dataframe
data_path = "./cleaned_csv/clean_listing_csv"  
listing_df = spark.read.csv(data_path, header=True, inferSchema=True)

#review calendar
data_path = "./cleaned_csv/clean_reviews_csv"  
review_df = spark.read.csv(data_path, header=True, inferSchema=True)



# %% [markdown]
# ## Dumping to postgres from spark
# 

# %%
# Writing

# calendar_df write
calendar_df.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/postgres',driver = 'org.postgresql.Driver', dbtable = 'calendar_df', user = user ,password = password).mode('overwrite').save()

# listing_df write
listing_df.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/postgres',driver = 'org.postgresql.Driver', dbtable = 'listing_df', user = user,password = password).mode('overwrite').save()

# review_df write
review_df.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/postgres',driver = 'org.postgresql.Driver', dbtable = 'review_df', user = user, password = password).mode('overwrite').save()

# %% [markdown]
# ## Importing datasets from postgres to work on questions

# %%
calendar_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "calendar_df") \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "org.postgresql.Driver") \
    .load()
calendar_df.show(truncate=False)

# %%
listing_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "listing_df") \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "org.postgresql.Driver") \
    .load()
listing_df.show(truncate=False)


# %%
review_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "review_df") \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "org.postgresql.Driver") \
    .load()
review_df.show(truncate=False)

# %% [markdown]
# ## Questions:

# %% [markdown]
# ## 1. Host Portfolio Growth Analysis: Identify hosts who have significantly grown their portfolio over time by calculating the percentage increase in the number of listings they manage. Analyze any common characteristics among these hosts.

# %%
import pyspark.sql.functions as F

window_spec = Window.partitionBy("host_name").orderBy("host_since")

# Calculate the number of listings each month for each host
listing_df_date = listing_df.withColumn("month", F.month("host_since"))
listing_df_date = listing_df_date.withColumn("year", F.year("host_since"))

listing_df_date = listing_df_date.withColumn("listing_count", F.row_number().over(window_spec))

# listing_df_date.show()

# Calculate the percentage increase in listings count for each host each month
listing_df_inc = listing_df_date.withColumn("prev_listing_count", F.lag("listing_count").over(window_spec))
listing_df_inc = listing_df_inc.withColumn("portfolio_growth",
                                   ((listing_df_inc["listing_count"] - listing_df_inc["prev_listing_count"]) / listing_df_inc["prev_listing_count"]) * 100)

# Define a threshold for significant growth (e.g., 50%)
threshold = 10

# Filter hosts with significant growth
significant_growth_hosts = listing_df_inc.filter(listing_df_inc["portfolio_growth"] > threshold)

# Select common characteristics of these hosts
common_characteristics = significant_growth_hosts.select("id", "host_name", "property_type", "host_location", "price", "host_since", "portfolio_growth")

common_characteristics.show(truncate=False)

# common_characteristics.select(F.col("portfolio_growth")).distinct().show()

# common_characteristics.select(col("host_name")).distinct().show()

# %% [markdown]
# ## 2. Calculate the booking rate (percentage of available dates booked) for each listing, considering the day of the week. Identify listings with significant booking rate fluctuations based on the day of the week. Also, display the day with highest average booking rate and the create a pivot that shows days of a week and the average booking rates for each.

# %%
# Extract the day of the week from the date column
calendar_df_date = calendar_df.withColumn("day_of_week", date_format(col("date"), "E"))

# Define a window specification to calculate cumulative available and booked days
window_spec = Window.partitionBy("listing_id", "day_of_week").orderBy("date")

# Calculate cumulative available and booked days
calendar_df_cum = calendar_df_date.withColumn("cumulative_available", count(when(col("available") == "true", 1)).over(window_spec))
calendar_df_cum = calendar_df_cum.withColumn("cumulative_booked", count(when(col("available") == "false", 1)).over(window_spec))

# Calculate the booking rate (percentage of available dates booked) for each day of the week
calendar_df_rate = calendar_df_cum.withColumn("booking_rate", (col("cumulative_booked") / (col("cumulative_available") + col("cumulative_booked"))) * 100)

# Calculate the standard deviation of booking rates for each listing and day of the week
stddev_window = Window.partitionBy("listing_id", "day_of_week")
calendar_df_stddev = calendar_df_rate.withColumn("booking_rate_stddev", stddev(col("booking_rate")).over(stddev_window))
calendar_df_mean = calendar_df_rate.withColumn("booking_rate_mean", mean(col("booking_rate")).over(stddev_window))

threshold = 29  

fluctuating_listings = calendar_df_stddev.filter(col("booking_rate_stddev") > threshold)

# fluctuating_listings.select(col("booking_rate_stddev")).distinct().show()

fluctuating_listings.show()

# avg booking rate for each day of the week
avg_booking_rate_by_day = calendar_df_rate.groupBy("day_of_week") \
    .agg(avg("booking_rate").alias("avg_booking_rate"))

# day with the highest avg booking rate
highest_avg_booking_day = avg_booking_rate_by_day.orderBy(col("avg_booking_rate").desc()).first()

print("Day with the highest average booking rate:", highest_avg_booking_day["day_of_week"])

# Pivot table to show days of a week and their total booking rates.
pivot_table = avg_booking_rate_by_day.withColumnRenamed("day_of_week", "DayOfWeek").withColumnRenamed("avg_booking_rate", "TotalBookingRate")
pivot_table.show()

# %% [markdown]
# ## Dumping the output dataframes to postgres
# 

# %%
# Writing outputs to beaver

# output for first question write
common_characteristics.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/postgres',driver = 'org.postgresql.Driver', dbtable = 'output_1', user = user, password = password).mode('overwrite').save()

# output for second question write
fluctuating_listings.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/postgres',driver = 'org.postgresql.Driver', dbtable = 'output_2', user = user, password = password).mode('overwrite').save()

# output for second question write
pivot_table.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/postgres',driver = 'org.postgresql.Driver', dbtable = 'output_2_1', user = user ,password = password).mode('overwrite').save()

# %%



