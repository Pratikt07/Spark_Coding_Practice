package set_1_when_otherwise_groupBy

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/*

Question 1: Employee Status Check
Create a DataFrame that lists employees with names and their work status. For each employee,
determine if they are “Active” or “Inactive” based on the last check-in date. If the check-in date is
within the last 7 days, mark them as "Active"; otherwise, mark them as "Inactive." Ensure the first
letter of each name is capitalized.


 */
object Q1 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Q1").master("local[*]").getOrCreate()

    import spark.implicits._

    val employees = List(
      ("karthik", "2024-11-01"),
      ("neha", "2024-10-20"),
      ("priya", "2024-10-28"),
      ("mohan", "2024-11-02"),
      ("ajay", "2024-09-15"),
      ("vijay", "2024-10-30"),
      ("veer", "2024-10-25"),
      ("aatish", "2024-10-10"),
      ("animesh", "2024-10-15"),
      ("nishad", "2024-11-01"),
      ("varun", "2024-10-05"),
      ("aadil", "2024-09-30")
    ).toDF("name", "last_checkin")

    employees.select(initcap(col("name")), when(
      datediff(current_date(), col("last_checkin")) <= 7, "Active").otherwise("Inactive").alias("status"))show()

    employees.createTempView("emp")

    spark.sql(
      """select upper(name) as name,
        | case when datediff(current_date(), cast(last_checkin as timestamp) ) <= 7 then 'Active'
        | else 'Inactive'
        | end as status
        | from emp""".stripMargin).show()
  }
}



