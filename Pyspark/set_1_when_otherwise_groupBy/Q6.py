'''

    7. Vehicle Mileage Analysis
      Classify each vehicle’s mileage as "High Efficiency" if mileage is above 25 MPG, "Moderate Efficiency"
    if between 15-25 MPG, and "Low Efficiency" if below 15 MPG.

'''
vehicles = [
    ("CarA", 30),
    ("CarB", 22),
    ("CarC", 18),
    ("CarD", 15),
    ("CarE", 10),
    ("CarF", 28),
    ("CarG", 12),
    ("CarH", 35),
    ("CarI", 25),
    ("CarJ", 16)
]
vehicles_df = spark.createDataFrame(vehicles, ["vehicle_name", "mileage"]
