package set_1_when_otherwise_groupBy

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Q16 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Q16").master("local[*]").getOrCreate()

    import spark.implicits._

    /*
    Scenario 17: Employee Salary Band and Performance Classification
      Question Set:
       1. Classify employees into salary bands: "Senior" if salary > 100k and experience > 10
      years, "Mid-level" if salary between 50-100k and experience 5-10 years, and "Junior" otherwise.
      Group by department to find count of each salary band.
       2. For each salary band, calculate the average performance score. Filter for bands where
      average performance exceeds 80.
       3. Find employees in "Mid-level" band with performance above 85 and experience over 7 years

    */

    val employees = List(
      ("karthik", "IT", 110000, 12, 88),
      ("neha", "Finance", 75000, 8, 70),
      ("priya", "IT", 50000, 5, 65),
      ("mohan", "HR", 120000, 15, 92),
      ("ajay", "IT", 45000, 3, 50),
      ("vijay", "Finance", 80000, 7, 78),
      ("veer", "Marketing", 95000, 6, 85),
      ("aatish", "HR", 100000, 9, 82),
      ("animesh", "Finance", 105000, 11, 88),
      ("nishad", "IT", 30000, 2, 55)
    ).toDF("name", "department", "salary", "experience", "performance_score")

    val empClassificationDF = employees.withColumn("salary_band", when(col("salary") > 100000 && col("experience") > 10, "Senior")
      .when(col("salary").between(50000, 100000) && col("experience").between(5, 10), "Mid-level")
      .otherwise("junior"))


    empClassificationDF.show()

    empClassificationDF.groupBy(col("department"), col("salary_band")).agg(count(col("salary_band")).alias("salary_band_count")).show()

    empClassificationDF.groupBy(col("salary_band")).agg(avg(col("performance_score")).alias("avg_performance_score"))
      .orderBy(col("salary_band"))
      .filter(col("avg_performance_score") > 80)
      .show()

    empClassificationDF.filter(col("salary_band") === "Mid level" && col("performance_score") > 85 && col("experience") > 7).show()

//    --------------------------------------------------------------------------------------------------------------
//    SQL  Solution

    employees.createTempView("emp")

    val sqlEmpSalaryBandDF = spark.sql(
      """
        | select *,
        | case
        |   when salary > 100000 and experience > 10 then 'Senior'
        |   when ( salary between 50000 and 100000) and ( experience between 5 and 10) then 'Mid-level'
        |   else 'Junior'
        | end as salary_band
        | from emp
        |""".stripMargin)

    sqlEmpSalaryBandDF.show()

    sqlEmpSalaryBandDF.createTempView("emp_bands")

    spark.sql(
      """
        | select department, salary_band, count(salary_band) from emp_bands group by department, salary_band
        |""".stripMargin).show()

    spark.sql(
      """
        | select salary_band, avg(performance_score) as avg_performance_score from emp_bands group by salary_band having avg_performance_score > 80
        |""".stripMargin).show()

    spark.sql(
      """
        | select * from emp_bands where salary_band = 'Mid-level' and performance_score > 80  and experience > 7
        |""".stripMargin)


  }

}
