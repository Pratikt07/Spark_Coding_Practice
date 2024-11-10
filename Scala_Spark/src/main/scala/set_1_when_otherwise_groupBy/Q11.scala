package set_1_when_otherwise_groupBy

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Q11 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Q11").master("local[*]").getOrCreate()

    import spark.implicits._
    /*
      12. Customer Spending Pattern Based on Age and Membership Level

      Classify customers' spending as "High Spender" if spending exceeds $1000 with "Premium"
      membership, "Average Spender" if spending between $500-$1000 and membership is "Standard",
      and "Low Spender" otherwise. Group by membership and calculate average spending

      Expected Output: Displays customers' names, spending category, and average spending by membership type.

     */

    val customers = List(
      ("karthik", "Premium", 1050, 32),
      ("neha", "Standard", 800, 28),
      ("priya", "Premium", 1200, 40),
      ("mohan", "Basic", 300, 35),
      ("ajay", "Standard", 700, 25),
      ("vijay", "Premium", 500, 45),
      ("veer", "Basic", 450, 33),
      ("aatish", "Standard", 600, 29),
      ("animesh", "Premium", 1500, 60),
      ("nishad", "Basic", 200, 21)
    ).toDF("name", "membership", "spending", "age")

    val cust_spending_category = customers.select(col("*"),
                                  when(col("membership") === "Premium" && col("spending") > 1000, "High Spender")
                                  .when(col("membership") === "Standard" &&
                                    ( col("spending") < 1000 && col("spending") > 500 ), "Average Spender")
                                  .otherwise("Low Spender").alias("spending_category") )
    cust_spending_category.show()

    cust_spending_category.groupBy(col("membership")).agg(avg(col("spending")).alias("Average Spending")).show()

    customers.createTempView("cust")

    val cust_spend_category_sql = spark.sql(
      """
        | select *,
        | case
        |   when membership = 'Premium' and spending > 1000 then 'High Spender'
        |   when membership = 'Standard' and spending < 1000 and spending > 500 then 'Average Spender'
        |   else 'Low Spender'
        | end as spend_category
        | from cust
        |""".stripMargin)

    cust_spend_category_sql.show()

    cust_spend_category_sql.createTempView("cust_spend_category")

    spark.sql(
      """
        | select membership,
        | avg(spending)
        | from cust_spend_category
        | group by membership
        |
        |""".stripMargin).show()


  }

}
