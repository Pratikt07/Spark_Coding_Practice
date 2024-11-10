package set_1_when_otherwise_groupBy

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Q20 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Q20").master("local[*]").getOrCreate()

    import spark.implicits._
    /*

        Scenario 21: Employee Productivity Scoring
        Question Set:
        1. Classify employees as "High Performer" if productivity score > 80 and project count
          is greater than 5, "Average Performer" if productivity score is between 60 and 80, and "Low
        Performer" otherwise. Count employees in each classification.
        2. Calculate the average productivity score for "High Performer" employees and the minimum
        score for "Average Performers."
        3. Identify "Low Performers" with a productivity score below 50 and project count under 2.
    */

    val employeeProductivity = List(
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
    ).toDF("employee_id", "productivity_score", "project_count")

    val empCatDF = employeeProductivity.withColumn("emp_category", when(col("productivity_score") > 80 && col("project_count") > 5, "High Performer")
      .when(col("productivity_score").between(60, 80), "Average Performer")
      .otherwise("Low Performer"))

    empCatDF.show()

    empCatDF.groupBy(col("emp_category")).agg(count(col("employee_id")).alias("employee_count")).show()

    empCatDF.groupBy(col("emp_category")).agg(avg(col("productivity_score")).alias("avg_productivity_score")).filter(col("emp_category") === "High Performer").show()

    empCatDF.groupBy(col("emp_category")).agg(min(col("productivity_score")).alias("min_productivity_score")).filter(col("emp_category") === "Average Performer").show()

    empCatDF.filter(
      col("productivity_score") < 50 &&
        col("project_count") < 2 &&
        col("emp_category") === "Low Performer"
    ).show()

    employeeProductivity.createOrReplaceTempView("employeeProductivity")

    val empCatSQL = spark.sql(
      """
  SELECT *,
         CASE
           WHEN productivity_score > 80 AND project_count > 5 THEN 'High Performer'
           WHEN productivity_score BETWEEN 60 AND 80 THEN 'Average Performer'
           ELSE 'Low Performer'
         END AS emp_category
  FROM employeeProductivity
""")
    empCatSQL.show()

    empCatSQL.createTempView("empCat")

    spark.sql(
      """
  SELECT emp_category, COUNT(employee_id) AS employee_count
  FROM empCat
  GROUP BY emp_category
""").show()

    spark.sql(
      """
  SELECT emp_category, AVG(productivity_score) AS avg_productivity_score
  FROM empCat
  WHERE emp_category = 'High Performer'
  GROUP BY emp_category
""").show()

    spark.sql(
      """
  SELECT emp_category, AVG(productivity_score) AS avg_productivity_score
  FROM empCat
  WHERE emp_category = 'High Performer'
  GROUP BY emp_category
""").show()

    spark.sql(
      """
  SELECT *
  FROM empCat
  WHERE productivity_score < 50
    AND project_count < 2
    AND emp_category = 'Low Performer'
""").show()


  }

}
