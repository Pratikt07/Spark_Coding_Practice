package set_1_when_otherwise_groupBy

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Q23 {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder().appName("Q22").master("local[*]").getOrCreate()

    import spark.implicits._
    /*

        Scenario 24: Student Graduation Prediction
        Question Set:

        1. Classify students as "At-Risk" if attendance is below 75% and the average test score
        is below 50, "Moderate Risk" if attendance is between 75% and 85%, and "Low Risk" otherwise.
        Calculate the number of students in each risk category.
        2. Find the average score for students in the "At-Risk" category.
        3. Identify "Moderate Risk" students who have scored above 70 in at least three subjects.

    */

    val students = List(
      ("Student1", 70, 45, 60, 65, 75),
      ("Student2", 80, 55, 58, 62, 67),
      ("Student3", 65, 90, 45, 78, 85),
      ("Student4", 90, 85, 80, 78, 76),
      ("Student5", 72, 40, 50, 48, 52),
      ("Student6", 88, 60, 72, 70, 68),
      ("Student7", 74, 48, 62, 66, 70),
      ("Student8", 82, 56, 64, 60, 66),
      ("Student9", 78, 50, 48, 58, 55),
      ("Student10", 68, 35, 42, 52, 45)
    ).toDF("student_id", "attendance_percentage", "math_score", "science_score", "english_score",
      "history_score")

    val avgTestScoreDF = students.withColumn("avg_score", (col("math_score") + col("science_score") + col("english_score") + col("history_score")) / 4)

    avgTestScoreDF.show()

    val studCatDF = avgTestScoreDF.withColumn("risk_category", when(col("attendance_percentage") < 75 && col("avg_score") < 50, "At-Risk")
      .when(col("attendance_percentage").between(75, 85), "Moderate Risk").otherwise("Low Risk"))

    studCatDF.show()

    studCatDF.filter(col("risk_category") === "At-Risk").show()

    studCatDF.withColumn("no_of_scores_more_than_70", (col("math_score") > 70).cast("Int") +
        (col("science_score") > 70).cast("Int") +
        (col("english_score") > 70).cast("Int") +
        (col("history_score") > 70).cast("Int"))
      .filter(col("no_of_scores_more_than_70") >= 3).show()

    //    ----------------------------------------------------------------------------------------------------------------------------------------
    //    SQL Solution
    val avgTestScoreSQL = students.withColumn("avg_score", (col("math_score") + col("science_score") + col("english_score") + col("history_score")) / 4)

    avgTestScoreSQL.show()

    // Create a temporary view
    avgTestScoreDF.createOrReplaceTempView("avgTestScore")

    val studCatSQL = spark.sql(
      """
  SELECT *,
         CASE
             WHEN attendance_percentage < 75 AND avg_score < 50 THEN 'At-Risk'
             WHEN attendance_percentage BETWEEN 75 AND 85 THEN 'Moderate Risk'
             ELSE 'Low Risk'
         END AS risk_category
  FROM avgTestScore
""")

    studCatSQL.createTempView("studCat")

    val atRiskDF = spark.sql(
      """
  SELECT *
  FROM studCat
  WHERE risk_category = 'At-Risk'
""")

    atRiskDF.show()

    val highScoresDF = spark.sql(
      """
  SELECT *,
         (CAST(math_score > 70 AS INT) +
          CAST(science_score > 70 AS INT) +
          CAST(english_score > 70 AS INT) +
          CAST(history_score > 70 AS INT)) AS no_of_scores_more_than_70
  FROM studCat
""")

    highScoresDF.createOrReplaceTempView("highScores")

    // Filter where `no_of_scores_more_than_70` >= 3
    val finalDF = spark.sql(
      """
  SELECT *
  FROM highScores
  WHERE no_of_scores_more_than_70 >= 3
""")

    finalDF.show()


  }

}
