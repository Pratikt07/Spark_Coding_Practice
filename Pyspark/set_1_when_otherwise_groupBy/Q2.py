from pyspark.sql import SparkSession
from pyspark.sql.functions import *

'''
Question 2: Sales Performance by Agent
Given a DataFrame of sales agents with their total sales amounts, calculate the performance status
based on sales thresholds: “Excellent” if sales are above 50,000, “Good” if between 25,000 and
50,000, and “Needs Improvement” if below 25,000. Capitalize each agent's name, and show total
sales aggregated by performance status.

'''

sales = [
    ("karthik", 60000),
    ("neha", 48000),
    ("priya", 30000),
    ("mohan", 24000),
    ("ajay", 52000),
    ("vijay", 45000),
    ("veer", 70000),
    ("aatish", 23000),
    ("animesh", 15000),
    ("nishad", 8000),
    ("varun", 29000),
    ("aadil", 32000)
]

spark = SparkSession.builder.appName("Q2").getOrCreate()

sales_df = spark.createDataFrame(sales, ["name", "total_sales"])

