
'''
   9. Product Inventory Check
      Classify inventory stock levels as "Overstocked" if stock exceeds 100, "Normal" if between 50-100,
    and "Low Stock" if below 50. Aggregate total stock in each category.
'''

inventory = [
("ProductA", 120),
("ProductB", 95),
("ProductC", 45),
("ProductD", 200),
("ProductE", 75),
("ProductF", 30),
("ProductG", 85),
("ProductH", 100),
("ProductI", 60),
("ProductJ", 20)
]
inventory_df = spark.createDataFrame(inventory, ["product_name", "stock_quantity"