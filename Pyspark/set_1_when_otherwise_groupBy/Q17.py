
'''

     Scenario 18: Product Sales Analysis
      Question Set:
      1. Classify products as "Top Seller" if total sales exceed 200,000 and discount offered is less
      than 10%, "Moderate Seller" if total sales are between 100,000 and 200,000, and "Low
      Seller" otherwise. Count the total number of products in each classification.
      2. Find the maximum sales value among "Top Seller" products and the minimum discount rate
      among "Moderate Seller" products.
      3. Identify products from the "Low Seller" category with a total sales value below 50,000 and
      discount offered above 15%.

'''
product_sales = [
    ("Product1", 250000, 5),
    ("Product2", 150000, 8),
    ("Product3", 50000, 20),
    ("Product4", 120000, 10),
    ("Product5", 300000, 7),
    ("Product6", 60000, 18),
    ("Product7", 180000, 9),
    ("Product8", 45000, 25),
    ("Product9", 70000, 15),
    ("Product10", 10000, 30)
]
product_sales_df = spark.createDataFrame(product_sales, ["product_name", "total_sales",
                                                         "discount"])
