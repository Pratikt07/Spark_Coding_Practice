'''
6. Customer Age Grouping

      Group customers as "Youth" if under 25, "Adult" if between 25-45, and "Senior" if over 45. Capitalize
      names and show total customers in each group.
'''

customers = [
    ("karthik", 22),
    ("neha", 28),
    ("priya", 40),
    ("mohan", 55),
    ("ajay", 32),
    ("vijay", 18),
    ("veer", 47),
    ("aatish", 38),
    ("animesh", 60),
    ("nishad", 25)
]
customers_df = spark.createDataFrame(customers, ["name", "age"])
