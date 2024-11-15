'''
    Scenario 22: Banking Fraud Detection
    Question Set:
      1. Classify transactions as "High Risk" if the transaction amount is above 10,000 and frequency
      of transactions from the same account within a day exceeds 5, "Moderate Risk" if the
      amount is between 5,000 and 10,000 and frequency is between 2 and 5, and "Low Risk"
      otherwise. Calculate the total number of transactions in each risk level.
      2. Identify accounts with at least one "High Risk" transaction and the total amount transacted
      by those accounts.
      3. Find all "Moderate Risk" transactions where the account type is "Savings" and the amount is
      above 7,500.
'''

transactions = [
    ("Account1", "2024-11-01", 12000, 6, "Savings"),
    ("Account2", "2024-11-01", 8000, 3, "Current"),
    ("Account3", "2024-11-02", 2000, 1, "Savings"),
    ("Account4", "2024-11-02", 15000, 7, "Savings"),
    ("Account5", "2024-11-03", 9000, 4, "Current"),
    ("Account6", "2024-11-03", 3000, 1, "Current"),
    ("Account7", "2024-11-04", 13000, 5, "Savings"),
    ("Account8", "2024-11-04", 6000, 2, "Current"),
    ("Account9", "2024-11-05", 20000, 8, "Savings"),
    ("Account10", "2024-11-05", 7000, 3, "Savings")
]
transactions_df = spark.createDataFrame(transactions, ["account_id", "transaction_date", "amount",
                                                       "frequency", "account_type"])
