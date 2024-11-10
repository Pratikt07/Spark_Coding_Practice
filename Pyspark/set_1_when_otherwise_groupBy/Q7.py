
'''

  8. Student Grade Classification

      Classify students based on their scores as "Excellent" if score is 90 or above, "Good" if between 75-
      89, and "Needs Improvement" if below 75. Count students in each category.

'''

students = [
("karthik", 95),
("neha", 82),
("priya", 74),
("mohan", 91),
("ajay", 67),
("vijay", 80),
("veer", 85),
("aatish", 72),
("animesh", 90),
("nishad", 60)
]
students_df = spark.createDataFrame(students, ["name", "score"]