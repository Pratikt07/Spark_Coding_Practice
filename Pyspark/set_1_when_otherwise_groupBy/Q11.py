'''
      12. Customer Spending Pattern Based on Age and Membership Level

      Classify customers' spending as "High Spender" if spending exceeds $1000 with "Premium"
      membership, "Average Spender" if spending between $500-$1000 and membership is "Standard",
      and "Low Spender" otherwise. Group by membership and calculate average spending

      Expected Output: Displays customers' names, spending category, and average spending by membership type.
'''

customers = [
    ("karthik", "Premium", 1050, 32),
    ("neha", "Standard", 800, 28),
    ("priya", "Premium", 1200, 40),
    ("mohan", "Basic", 300, 35),
    ("ajay", "Standard", 700, 25),
    ("vijay", "Premium", 500, 45),
    ("veer", "Basic", 450, 33),
    ("aatish", "Standard", 600, 29),
    ("animesh", "Premium", 1500, 60),
    ("nishad", "Basic", 200, 21)
]
customers_df = spark.createDataFrame(customers, ["name", "membership", "spending", "age"])
