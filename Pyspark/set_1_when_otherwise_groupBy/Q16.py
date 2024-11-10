employees = [
    ("karthik", "IT", 110000, 12, 88),
    ("neha", "Finance", 75000, 8, 70),
    ("priya", "IT", 50000, 5, 65),
    ("mohan", "HR", 120000, 15, 92),
    ("ajay", "IT", 45000, 3, 50),
    ("vijay", "Finance", 80000, 7, 78),
    ("veer", "Marketing", 95000, 6, 85),
    ("aatish", "HR", 100000, 9, 82),
    ("animesh", "Finance", 105000, 11, 88),
    ("nishad", "IT", 30000, 2, 55)
]

employees = spark.createDataFrame(employees, ["name", "department", "salary", "experience", "performance_score"])