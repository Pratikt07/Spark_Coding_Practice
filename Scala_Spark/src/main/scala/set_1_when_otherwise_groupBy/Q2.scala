package set_1_when_otherwise_groupBy

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object Q2 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Q2").master("local[*]").getOrCreate()

    import spark.implicits._

    val sales = List(
      ("karthik", 60000),
      ("neha", 48000),
      ("priya", 30000),
      ("mohan", 24000),
      ("ajay", 52000),
      ("vijay", 45000),
      ("veer", 70000),
      ("aatish", 23000),
      ("animesh", 15000),
      ("nishad", 8000),
      ("varun", 29000),
      ("aadil", 32000)
    ).toDF("name", "total_sales")

    val sales_with_status = sales.select(initcap(col("name")).alias("name"), col("total_sales"), when ( col("total_sales") > 50000, "Excellent").when(col("total_sales") > 25000 && col("total_sales") < 50000, "Good" ).otherwise("Needs Improvement").alias("performance_status"))

    sales_with_status.show()

    val agg_status_sales = sales_with_status.groupBy("performance_status").agg(sum(col("total_sales")).alias("agg_status_sales") )

    agg_status_sales.show()

    sales.createOrReplaceTempView("sales")
    val sales_sql_status = spark.sql(
      """
        | select
        |   concat( upper( substring(name,1,1) ), lower( substring(name,2) ) ) as name,
        |   total_sales,
        |   case
        |      when total_sales > 50000 then 'Excellent'
        |      when total_sales > 25000 and total_sales < 50000 then 'Good'
        |      else 'Need Improvement'
        |   end as performance_status
        | from sales
        |""".stripMargin)

    sales_sql_status.show()

    sales_sql_status.createOrReplaceTempView("sales_status")

    spark.sql(
      """
        | select
        |   performance_status,
        |   sum(total_sales)
        |  from sales_status
        |  group by performance_status
        |""".stripMargin).show()

  }

}

/*

Question 2: Sales Performance by Agent
Given a DataFrame of sales agents with their total sales amounts, calculate the performance status
based on sales thresholds: “Excellent” if sales are above 50,000, “Good” if between 25,000 and
50,000, and “Needs Improvement” if below 25,000. Capitalize each agent's name, and show total
sales aggregated by performance status.


 */