package set_1_when_otherwise_groupBy

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Q10 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Q10").master("local[*]").getOrCreate()

    import spark.implicits._

    /*
      11. Product Return Analysis with Multi-Level Classification

      For each product, classify return reasons as "High Return Rate" if return count exceeds 100 and
      satisfaction score below 50, "Moderate Return Rate" if return count is between 50-100 with a score
      between 50-70, and "Low Return Rate" otherwise. Group by category to count product return rates.

      Expected Output: Shows the product name, return rate classification, and total number of
                        products in each return rate category for each category


     */
    val products = List(
      ("Laptop", "Electronics", 120, 45),
      ("Smartphone", "Electronics", 80, 60),
      ("Tablet", "Electronics", 50, 72),
      ("Headphones", "Accessories", 110, 47),
      ("Shoes", "Clothing", 90, 55),
      ("Jacket", "Clothing", 30, 80),
      ("TV", "Electronics", 150, 40),
      ("Watch", "Accessories", 60, 65),
      ("Pants", "Clothing", 25, 75),
      ("Camera", "Electronics", 95, 58)
    ).toDF("product_name", "category", "return_count", "satisfaction_score")


    val product_with_return_rates = products.select(col("*"), when(col("return_count") > 100 && col("satisfaction_score") < 50, "High Return Rate")
        .when( (col("return_count") > 50 && col("return_count") < 100) && (col("satisfaction_score") < 70 && col("satisfaction_score") > 50), "Moderate Return Rate")
        .otherwise("Low Return Rate").alias("return_rates") )

    product_with_return_rates.show()

    product_with_return_rates.
      groupBy("return_rates").agg(count(col("return_rates")).alias("return_rates_count"), sum(col("")) ).show()

    products.createTempView("products")

    val products_with_rates_sql = spark.sql(
      """
        | select *,
        |   case
        |     when return_count > 100 and satisfaction_score < 50 then 'High Return Rate'
        |     when return_count between 50 and 100 and satisfaction_score between 50 and 70 then 'Moderate Return Rate'
        |     else 'Low Return Rate'
        |  end as return_rates
        | from products
        |""".stripMargin)

    products_with_rates_sql.show()

    products_with_rates_sql.createTempView("products_rates")

    spark.sql(
      """
        | select
        |  return_rates,
        |  count(return_rates) as return_rates_count
        | from products_rates
        | group by return_rates
        |""".stripMargin).show()


  }

}
