package set_1_when_otherwise_groupBy

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Q9 {
  def main(args: Array[String]): Unit = {

    /*
      10. Employee Bonus Calculation Based on Performance and Department
Classify employees for a bonus eligibility program. Employees in "Sales" and "Marketing" with
performance scores above 80 get a 20% bonus, while others with scores above 70 get 15%. All other
employees receive no bonus. Group by department and calculate total bonus allocation.

      Expected Output: Grouped by department, showing names, bonus percentage, and total bonus amount by department

     */
    val spark = SparkSession.builder().appName("Q9").master("local[*]").getOrCreate()

    import spark.implicits._

    val employees = List(
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
    ).toDF("name", "department", "performance_score")

    val emp_perf_bonus_df = employees.select(col("name"), col("department"), col("performance_score"),
               when(col("department").isin("Sales", "Marketing") && col("performance_score") > 80, 20)
              .when(col("department").isin("Sales", "Marketing") && col("performance_score") > 70, 15)
              .otherwise(0).alias("performance_bonus") )

    emp_perf_bonus_df.show()

    emp_perf_bonus_df.groupBy(col("department")).agg(sum(col("performance_bonus")).alias("total_bonus_allocation")).show()

    employees.createTempView("emp")

    val emp_with_bonus_sql_df = spark.sql(
      """
        | select
        |   name,
        |   department,
        |   performance_score,
        |   case
        |     when department in ('Sales','Marketing') and performance_score > 80 then 20
        |     when department in ('Sales','Marketing') and performance_score > 70 then 15
        |     else 0
        |   end as performance_bonus
        | from emp
        |""".stripMargin)

    emp_with_bonus_sql_df.show()

    emp_with_bonus_sql_df.createTempView("emp_bonus")

    spark.sql(
      """
        | select
        |  department,
        |  sum(performance_bonus) as total_bonus_allocation
        | from emp_bonus
        | group by department
        |""".stripMargin).show()



  }

}
