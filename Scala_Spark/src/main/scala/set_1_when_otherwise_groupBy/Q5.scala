package set_1_when_otherwise_groupBy

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Q5 {
  def main(args: Array[String]): Unit = {

    /*
      6. Customer Age Grouping

      Group customers as "Youth" if under 25, "Adult" if between 25-45, and "Senior" if over 45. Capitalize
      names and show total customers in each group.
       Scala Spark Data

     */

    val spark = SparkSession.builder().appName("Q5").master("local[*]").getOrCreate()

    import spark.implicits._

    val customers = List(
      ("karthik", 22),
      ("neha", 28),
      ("priya", 40),
      ("mohan", 55),
      ("ajay", 32),
      ("vijay", 18),
      ("veer", 47),
      ("aatish", 38),
      ("animesh", 60),
      ("nishad", 25)
    ).toDF("name", "age")

    val cus_age_group = customers.select(initcap(col("name")), when(col("age") > 45, "Senior")
      .when(col("age") < 45 && col("age") > 25, "Adult").otherwise("Youth").alias("age_group"))

    cus_age_group.show()

    cus_age_group.groupBy(col("age_group")).agg(count(col("age_group")).alias("age_group_count")).show()

    customers.createTempView("customer")

    val cus_age_group_sql = spark.sql(
      """
        | Select
        | upper(name) as name,
        | case
        |   when age > 45 then 'Senior'
        |   when age > 25 and age < 45 then 'Adult'
        |   else 'Youth'
        | end as age_group
        | from customer
        |""".stripMargin)

    cus_age_group_sql.show()

    cus_age_group_sql.createTempView("cus_age_group")

    spark.sql(
      """
        | select
        |   age_group,
        |   count(age_group) as age_group_count
        |  from cus_age_group
        | group by age_group
        |""".stripMargin).show()


  }

}
