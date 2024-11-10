package set_1_when_otherwise_groupBy

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession


object Q3 {
  def main(args: Array[String]): Unit = {

    /*
    Question 3: Project Allocation and Workload Analysis

    Given a DataFrame with project allocation data for multiple employees, determine each employee's
    workload level based on their hours worked in a month across various projects. Categorize
    employees as “Overloaded” if they work more than 200 hours, “Balanced” if between 100-200 hours,
    and “Underutilized” if below 100 hours. Capitalize each employee’s name, and show the aggregated
      workload status count by category.
     Scala Sample Data

     */

    val spark  = SparkSession.builder().master("local[*]").appName("Q3").getOrCreate()

    import spark.implicits._

    val workload = List(
      ("karthik", "ProjectA", 120),
      ("karthik", "ProjectB", 100),
      ("neha", "ProjectC", 80),
      ("neha", "ProjectD", 30),
      ("priya", "ProjectE", 110),
      ("mohan", "ProjectF", 40),
      ("ajay", "ProjectG", 70),
      ("vijay", "ProjectH", 150),
      ("veer", "ProjectI", 190),
      ("aatish", "ProjectJ", 60),
      ("animesh", "ProjectK", 95),
      ("nishad", "ProjectL", 210),
      ("varun", "ProjectM", 50),
      ("aadil", "ProjectN", 90)
    ).toDF("name", "project", "hours")

    val workload_category_df = workload.groupBy(col("name")).agg(sum(col("hours")).alias("agg_hours"))
          .select(initcap(col("name")).alias("name"),when(col("agg_hours")>200, "Overloaded")
                              .when(col("agg_hours")>100 && col("agg_hours")<200, "Balanced")
                              .otherwise("Underutilized").alias("workload_category"))
    workload_category_df.show()

    val workload_category_count_df = workload_category_df.groupBy("workload_category").agg(count(col("workload_category")).alias("category_count"))
    workload_category_count_df.show()

    workload.createOrReplaceTempView("workload")

    val worklod_catgry_sql = spark.sql(
      """
        | select
        |   concat(upper(substring(name,1,1)), lower(substring(name,2))) as name,
        |   case
        |     when hours > 200 then 'Overloaded'
        |     when hours > 100 and hours < 200 then 'Balanced'
        |     else 'Underutilized'
        |   end as workload_category
        | from workload
        |""".stripMargin)

    worklod_catgry_sql.show()

    worklod_catgry_sql.createTempView("workload_cat")

    spark.sql(
      """
        | Select
        |   workload_category,
        |   count(workload_category) as category_count
        |   from workload_cat
        |   group by workload_category
        |""".stripMargin).show()

  }

}
