from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, datediff, current_date, floor, months_between

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Customer Data Processing") \
    .getOrCreate()

# Path to your data file (replace with actual path)
file_path = "/path/to/customer_data.csv"

# Load data into a Spark DataFrame
customer_df = spark.read.csv(file_path, header=True, inferSchema=True, sep='|')

# Show the loaded data
customer_df.show()

# Convert string date columns to proper date types
customer_df = customer_df.withColumn("Open_Date", to_date(col("Open_Date"), 'yyyyMMdd')) \
                         .withColumn("Last_Consulted_Date", to_date(col("Last_Consulted_Date"), 'yyyyMMdd')) \
                         .withColumn("DOB", to_date(col("DOB"), 'ddMMyyyy'))

# Add 'Age' derived column
customer_df = customer_df.withColumn("Age", floor(months_between(current_date(), col("DOB")) / 12))

# Add 'Days_Since_Last_Consulted' derived column
customer_df = customer_df.withColumn("Days_Since_Last_Consulted", 
                                     datediff(current_date(), col("Last_Consulted_Date")))

# Show processed data
customer_df.show()

# Filter data by country and create separate DataFrames
india_df = customer_df.filter(col("Country") == "IND")
usa_df = customer_df.filter(col("Country") == "USA")
australia_df = customer_df.filter(col("Country") == "AU")

# Show the filtered data for India (for demonstration)
india_df.show()

# Keep the record with the latest Last_Consulted_Date for each customer
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Define window partitioned by Customer_Id and ordered by Last_Consulted_Date in descending order
window_spec = Window.partitionBy("Customer_Id").orderBy(col("Last_Consulted_Date").desc())

# Add row_number to rank by latest consultation date for each customer
customer_ranked = customer_df.withColumn("row_num", row_number().over(window_spec))

# Filter to keep only the latest record per customer
latest_customer_df = customer_ranked.filter(col("row_num") == 1).drop("row_num")

# Show the result
latest_customer_df.show()


# Save the filtered India DataFrame to a CSV file
india_df.write.csv("/path/to/processed_data/India_Customers.csv", header=True)

# Save other countries similarly
usa_df.write.csv("/path/to/processed_data/USA_Customers.csv", header=True)
australia_df.write.csv("/path/to/processed_data/Australia_Customers.csv", header=True)


# Save to a database (example with JDBC)
india_df.write \
    .format("jdbc") \
    .option("url", "jdbc:mysql://your-database-url") \
    .option("dbtable", "Table_India") \
    .option("user", "your-username") \
    .option("password", "your-password") \
    .save()



