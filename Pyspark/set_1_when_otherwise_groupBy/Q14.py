'''

    Scenario 15: Customer Purchase Recency Categorization

    Question Set: 4. Categorize customers based on purchase recency: "Frequent" if last purchase within
    30 days, "Occasional" if within 60 days, and "Rare" if over 60 days. Show the number of each
    category per membership type.

    5. Find the average total purchase amount for customers with "Frequent" purchase recency
    and "Premium" membership.

    6. For customers with "Rare" recency, calculate the minimum purchase amount across different
    membership types.

'''

customer_purchases = [
    ("karthik", "Premium", 50, 5000),
    ("neha", "Standard", 10, 2000),
    ("priya", "Premium", 65, 8000),
    ("mohan", "Basic", 90, 1200),
    ("ajay", "Standard", 25, 3500),
    ("vijay", "Premium", 15, 7000),
    ("veer", "Basic", 75, 1500),
    ("aatish", "Standard", 45, 3000),
    ("animesh", "Premium", 20, 9000),
    ("nishad", "Basic", 80, 1100)
]
customer_purchases_df = spark.createDataFrame(customer_purchases, ["name", "membership",
                                                                   "days_since_last_purchase", "total_purchase_amount"])
