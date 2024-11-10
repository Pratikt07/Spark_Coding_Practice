'''

    Scenario 19: Customer Loyalty Analysis
    Question Set: 4. Classify customers as "Highly Loyal" if purchase frequency is greater than 20 times
      and average spending is above 500, "Moderately Loyal" if frequency is between 10-20 times, and
    "Low Loyalty" otherwise. Count customers in each classification.
    5. Calculate the average spending of "Highly Loyal" customers and the minimum spending for
      "Moderately Loyal" customers.
    6. Identify "Low Loyalty" customers with an average spending less than 100 and purchase
    frequency under 5.

'''

customer_loyalty = [
    ("Customer1", 25, 700),
    ("Customer2", 15, 400),
    ("Customer3", 5, 50),
    ("Customer4", 18, 450),
    ("Customer5", 22, 600),
    ("Customer6", 2, 80),
    ("Customer7", 12, 300),
    ("Customer8", 6, 150),
    ("Customer9", 10, 200),
    ("Customer10", 1, 90)
]
customer_loyalty_df = spark.createDataFrame(customer_loyalty, ["customer_name",
                                                               "purchase_frequency", "average_spending"])
