'''

    Scenario 20: E-commerce Return Rate Analysis
      Question Set:
      7. Classify products by return rate: "High Return" if return rate is over 20%, "Medium
      Return" if return rate is between 10% and 20%, and "Low Return" otherwise. Count products in each
      classification.
      8. Calculate the average sale price for "High Return" products and the maximum return rate for
        "Medium Return" products.
      9. Identify "Low Return" products with a sale price under 50 and return rate less than 5%.

'''

ecommerce_return = [
    ("Product1", 75, 25),
    ("Product2", 40, 15),
    ("Product3", 30, 5),
    ("Product4", 60, 18),
    ("Product5", 100, 30),
    ("Product6", 45, 10),
    ("Product7", 80, 22),
    ("Product8", 35, 8),
    ("Product9", 25, 3),
    ("Product10", 90, 12)
]
ecommerce_return_df = spark.createDataFrame(ecommerce_return, ["product_name", "sale_price",
                                                               "return_rate"])
