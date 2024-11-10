'''
    Scenario 21: Employee Productivity Scoring
    Question Set:
    1. Classify employees as "High Performer" if productivity score > 80 and project count
      is greater than 5, "Average Performer" if productivity score is between 60 and 80, and "Low
    Performer" otherwise. Count employees in each classification.
    2. Calculate the average productivity score for "High Performer" employees and the minimum
    score for "Average Performers."
    3. Identify "Low Performers" with a productivity score below 50 and project count under 2.
'''

employee_productivity = [
    ("Emp1", 85, 6),
    ("Emp2", 75, 4),
    ("Emp3", 40, 1),
    ("Emp4", 78, 5),
    ("Emp5", 90, 7),
    ("Emp6", 55, 3),
    ("Emp7", 80, 5),
    ("Emp8", 42, 2),
    ("Emp9", 30, 1),
    ("Emp10", 68, 4)
]
employee_productivity_df = spark.createDataFrame(employee_productivity, ["employee_id",
                                                                         "productivity_score", "project_count"])
