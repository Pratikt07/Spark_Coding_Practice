package set_1_when_otherwise_groupBy

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Q18 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Q17").master("local[*]").getOrCreate()

    import spark.implicits._
/*

    Scenario 19: Customer Loyalty Analysis

    Question Set:
    1. Classify customers as "Highly Loyal" if purchase frequency is greater than 20 times
      and average spending is above 500, "Moderately Loyal" if frequency is between 10-20 times, and
    "Low Loyalty" otherwise. Count customers in each classification.
    2. Calculate the average spending of "Highly Loyal" customers and the minimum spending for
      "Moderately Loyal" customers.
    3. Identify "Low Loyalty" customers with an average spending less than 100 and purchase
    frequency under 5.
*/

    val customerLoyalty = List(
      ("Customer1", 25, 700),
      ("Customer2", 15, 400),
      ("Customer3", 5, 50),
      ("Customer4", 18, 450),
      ("Customer5", 22, 600),
      ("Customer6", 2, 80),
      ("Customer7", 12, 300),
      ("Customer8", 6, 150),
      ("Customer9", 10, 200),
      ("Customer10", 1, 90)
    ).toDF("customer_name", "purchase_frequency", "average_spending")

    val loyalCustDF = customerLoyalty.withColumn("loyalty", when(col("purchase_frequency") > 20 && col("average_spending") > 500, "Highly Loyal")
      .when(col("purchase_frequency").between(10,20), "Moderately Loyal")
      .otherwise("Low Loyalty") )

    loyalCustDF.show()

    loyalCustDF.groupBy(col("loyalty")).agg(count(col("customer_name")).alias("customer_count")).show()

    loyalCustDF.groupBy(col("loyalty")).agg(avg(col("average_spending")).alias("avg_spending")).filter(col("loyalty") === "Highly Loyal").show()

    loyalCustDF.groupBy(col("loyalty")).agg(min(col("average_spending")).alias("min_spending")).filter(col("loyalty") === "Moderately Loyal").show()

    loyalCustDF.filter(col("loyalty") === "Low Loyalty" && col("average_spending") < 100 && col("purchase_frequency")< 5).show()

//  --------------------------------------------------------------------------------------------------------------------------
//    SQL Solution
     customerLoyalty.createTempView("customer")

    val custLoyalty = spark.sql(
      """
        | select *,
        | case
        |   when purchase_frequency > 20 and average_spending > 500 then 'High Loyalty'
        |   when purchase_frequency between 10 and 20 then 'Moderately Loyalty'
        |   else 'Low Loyalty'
        | end as loyalty
        | from customer
        |""".stripMargin)

    custLoyalty.show()

    custLoyalty.createTempView("cust_loyalty")

    spark.sql(
      """
        | select loyalty, count(customer_name) from cust_loyalty group by loyalty
        |""".stripMargin).show()


    spark.sql(
      """
        | select loyalty, avg(average_spending) as avg_spending
        | from cust_loyalty
        | where loyalty = 'High Loyalty'
        | group by loyalty
        |""".stripMargin).show()

    spark.sql(
      """
        | select loyalty, min(average_spending) as min_spending
        | from cust_loyalty
        |  where loyalty = 'Moderately Loyalty'
        | group by loyalty
        |""".stripMargin).show()

    spark.sql(
      """
        | select * from cust_loyalty
        | where
        |   loyalty = 'Low Loyalty' and
        |   average_spending < 100 and
        |   purchase_frequency < 5
        |""".stripMargin).show()


  }

}
