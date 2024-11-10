'''
5. Overtime Calculation for Employees

Determine whether an employee has "Excessive Overtime" if their weekly hours exceed 60,
"Standard Overtime" if between 45-60 hours, and "No Overtime" if below 45 hours. Capitalize each
name and group by overtime status.
'''

employees = [
    ("karthik", 62),
    ("neha", 50),
    ("priya", 30),
    ("mohan", 65),
    ("ajay", 40),
    ("vijay", 47),
    ("veer", 55),
    ("aatish", 30),
    ("animesh", 75),
    ("nishad", 60)
]
employees_df = spark.createDataFrame(employees, ["name", "hours_worked"]
