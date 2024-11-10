'''

      Scenario 16: Electricity Consumption and Rate Assignment

      Question Set: 7. Classify households into "High Usage" if kWh exceeds 500 and bill exceeds $200,
      "Medium Usage" for kWh between 200-500 and bill between $100-$200, and "Low Usage"
      otherwise. Calculate the total number of households in each usage category.
      8. Find the maximum bill amount for "High Usage" households and calculate the average kWh
      for "Medium Usage" households.
      9. Identify households with "Low Usage" but kWh usage exceeding 300. Count such
      households.

'''


electricity_usage = [
("House1", 550, 250),
("House2", 400, 180),
("House3", 150, 50),
("House4", 500, 200),
("House5", 600, 220),
("House6", 350, 120),
("House7", 100, 30),
("House8", 480, 190),
("House9", 220, 105),
("House10", 150, 60)
]
electricity_usage_df = spark.createDataFrame(electricity_usage, ["household", "kwh_usage",
"total_bill"])