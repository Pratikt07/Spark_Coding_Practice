'''
      10. Employee Bonus Calculation Based on Performance and Department
Classify employees for a bonus eligibility program. Employees in "Sales" and "Marketing" with
performance scores above 80 get a 20% bonus, while others with scores above 70 get 15%. All other
employees receive no bonus. Group by department and calculate total bonus allocation.
'''


employees = [
("karthik", "Sales", 85),
("neha", "Marketing", 78),
("priya", "IT", 90),
("mohan", "Finance", 65),
("ajay", "Sales", 55),
("vijay", "Marketing", 82),
("veer", "HR", 72),
("aatish", "Sales", 88),
("animesh", "Finance", 95),
("nishad", "IT", 60)
]
employees_df = spark.createDataFrame(employees, ["name", "department", "performance_score"