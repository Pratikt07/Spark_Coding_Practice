'''
      13. E-commerce Order Fulfillment Timeliness Based on Product Type and Location

      Classify orders as "Delayed" if delivery time exceeds 7 days and origin location is "International",
      "On-Time" if between 3-7 days, and "Fast" if below 3 days. Group by product type to see the count of
      each delivery speed category.

      Expected Output: Orders categorized by delivery speed, showing the number of each type per product
'''

orders = [
    ("Order1", "Laptop", "Domestic", 2),
    ("Order2", "Shoes", "International", 8),
    ("Order3", "Smartphone", "Domestic", 3),
    ("Order4", "Tablet", "International", 5),
    ("Order5", "Watch", "Domestic", 7),
    ("Order6", "Headphones", "International", 10),
    ("Order7", "Camera", "Domestic", 1),
    ("Order8", "Shoes", "International", 9),
    ("Order9", "Laptop", "Domestic", 6),
    ("Order10", "Tablet", "International", 4)
]
orders_df = spark.createDataFrame(orders, ["order_id", "product_type", "origin", "delivery_days"])
