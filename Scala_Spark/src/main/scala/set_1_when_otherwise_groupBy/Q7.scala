package set_1_when_otherwise_groupBy

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Q7 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Q7").master("local[*]").getOrCreate()

    import spark.implicits._

    /*
    8. Student Grade Classification

      Classify students based on their scores as "Excellent" if score is 90 or above, "Good" if between 75-
      89, and "Needs Improvement" if below 75. Count students in each category.
     Scala Spark Data

     */
    val students = List(
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
    ).toDF("name", "score")

    val stud_cat_df = students.select(col("name"), when(col("score")>90, "Excellent").when(col("score")<75,"Needs Improvement")
    .otherwise("Good").alias("student_category"))
    stud_cat_df.show()

    stud_cat_df.groupBy(col("student_category")).agg(count(col("student_category")).alias("category_count")).show()

    students.createTempView("std")

    val std_cat_sql_df = spark.sql(
      """
        | select
        |   name,
        |   case
        |     when score > 90 then 'Excellent'
        |     when score < 75 then 'Needs Improvement'
        |     else 'Good'
        |    end as std_category
        | from std
        |""".stripMargin)

    std_cat_sql_df.show()

    std_cat_sql_df.createTempView("std_cat")

    spark.sql(
      """
        | select std_category,
        | count(std_category) as category_count
        | from std_cat
        | group by std_category
        |""".stripMargin).show()

  }
}
