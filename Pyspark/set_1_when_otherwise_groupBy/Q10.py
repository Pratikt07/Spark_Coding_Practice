'''
      11. Product Return Analysis with Multi-Level Classification

      For each product, classify return reasons as "High Return Rate" if return count exceeds 100 and
      satisfaction score below 50, "Moderate Return Rate" if return count is between 50-100 with a score
      between 50-70, and "Low Return Rate" otherwise. Group by category to count product return rates.

      Expected Output: Shows the product name, return rate classification, and total number of
                        products in each return rate category for each category
'''

products = [
("Laptop", "Electronics", 120, 45),
("Smartphone", "Electronics", 80, 60),
("Tablet", "Electronics", 50, 72),
("Headphones", "Accessories", 110, 47),
("Shoes", "Clothing", 90, 55),
("Jacket", "Clothing", 30, 80),
("TV", "Electronics", 150, 40),
("Watch", "Accessories", 60, 65),
("Pants", "Clothing", 25, 75),
("Camera", "Electronics", 95, 58)
]
products_df = spark.createDataFrame(products, ["product_name", "category", "return_count",
"satisfaction_score"])