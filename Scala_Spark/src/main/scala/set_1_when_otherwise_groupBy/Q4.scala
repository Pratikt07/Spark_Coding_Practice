package set_1_when_otherwise_groupBy

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Q4 {
  def main(args: Array[String]): Unit = {

    /*
5. Overtime Calculation for Employees

Determine whether an employee has "Excessive Overtime" if their weekly hours exceed 60,
"Standard Overtime" if between 45-60 hours, and "No Overtime" if below 45 hours. Capitalize each
name and group by overtime status.

     */

    val spark = SparkSession.builder().appName("Q4").master("local[*]").getOrCreate()

    import spark.implicits._

    val employees = List(
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
    ).toDF("name", "hours_worked")

    val overtime_status_df = employees.select(initcap(col("name")), when(col("hours_worked") > 60, "Excessive Overtime")
      .when(col("hours_worked") < 60 && col("hours_worked") > 45, "Standard Overtime").otherwise("No Overtime").alias("overtime_status"))

    overtime_status_df.show()

    overtime_status_df.groupBy(col("overtime_status")).agg(count(col("overtime_status"))).show()

    employees.createTempView("emp")

    val emp_overtime_sql_df = spark.sql(
      """
        | Select
        | upper(name) as name,
        | case
        |   when hours_worked > 60 then 'Excessive Overtime'
        |   when hours_worked >45 and hours_worked <60 then 'Standard Overtime'
        |   else 'No Overtime'
        | end as overtime_status
        | from emp
        |""".stripMargin)

    emp_overtime_sql_df.show()

    emp_overtime_sql_df.createTempView("emp_overtime_cat")

    spark.sql(
      """
        | select
        |   overtime_status,
        |   count(overtime_status) as overtime_status_count
        |  from emp_overtime_cat
        |  group by overtime_status
        |""".stripMargin).show()


  }

}
