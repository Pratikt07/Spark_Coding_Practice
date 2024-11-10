'''
    Scenario 24: Student Graduation Prediction
    Question Set:

    1. Classify students as "At-Risk" if attendance is below 75% and the average test score
    is below 50, "Moderate Risk" if attendance is between 75% and 85%, and "Low Risk" otherwise.
    Calculate the number of students in each risk category.
    2. Find the average score for students in the "At-Risk" category.
    3. Identify "Moderate Risk" students who have scored above 70 in at least three subjects.

'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

students = [
    ("Student1", 70, 45, 60, 65, 75),
    ("Student2", 80, 55, 58, 62, 67),
    ("Student3", 65, 30, 45, 70, 55),
    ("Student4", 90, 85, 80, 78, 76),
    ("Student5", 72, 40, 50, 48, 52),
    ("Student6", 88, 60, 72, 70, 68),
    ("Student7", 74, 48, 62, 66, 70),
    ("Student8", 82, 56, 64, 60, 66),
    ("Student9", 78, 50, 48, 58, 55),
    ("Student10", 68, 35, 42, 52, 45)
]

spark = SparkSession.builder.appName("dadudiiiiiiiiiii").getOrCreate()

students_df = spark.createDataFrame(students, ["student_id", "attendance_percentage",
                                               "math_score", "science_score", "english_score", "history_score"])

df1 = students_df.withColumn("avg_score",(col("math_score") + col("science_score") + col("english_score") + col("history_score")) / 4)
df1.show()

df = df1.withColumn(
    "status",
    when(
        (col("attendance_percentage") < 75) & (col("avg_score") < 50),
        "At-Risk"
    ).when(
        col("attendance_percentage").between(75, 85),
        "Moderate Risk"
    ).otherwise("Low Risk")
)


df.show()
