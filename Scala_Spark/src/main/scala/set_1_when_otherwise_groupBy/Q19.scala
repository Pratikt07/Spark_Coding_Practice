package set_1_when_otherwise_groupBy

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Q19 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Q19").master("local[*]").getOrCreate()

    import spark.implicits._
/*

    Scenario 20: E-commerce Return Rate Analysis
      Question Set:
      7. Classify products by return rate: "High Return" if return rate is over 20%, "Medium
      Return" if return rate is between 10% and 20%, and "Low Return" otherwise. Count products in each
      classification.
      8. Calculate the average sale price for "High Return" products and the maximum return rate for
        "Medium Return" products.
      9. Identify "Low Return" products with a sale price under 50 and return rate less than 5%.
*/

    val ecommerceReturn = List(
      ("Product1", 75, 25),
      ("Product2", 40, 15),
      ("Product3", 30, 5),
      ("Product4", 60, 18),
      ("Product5", 100, 30),
      ("Product6", 45, 10),
      ("Product7", 80, 22),
      ("Product8", 35, 8),
      ("Product9", 25, 3),
      ("Product10", 90, 12)
    ).toDF("product_name", "sale_price", "return_rate")

    val productReturnCatDF = ecommerceReturn.withColumn("return_rate_category", when(col("return_rate") > 20, "High Rate")
      .when(col("return_rate").between(10,20), "Moderate Rate").otherwise("Low Rate"))

    productReturnCatDF.show()

    productReturnCatDF.groupBy(col("return_rate_category")).agg(count(col("product_name")).alias("Product_Count")).show()

    productReturnCatDF.groupBy(col("return_rate_category")).agg(avg(col("return_rate")).alias("avg_return_rate")).where(col("return_rate_category") === "High Rate").show()

    productReturnCatDF.groupBy(col("return_rate_category")).agg(max(col("return_rate")).alias("max_return_rate")).where(col("return_rate_category") === "Moderate Rate").show()

    productReturnCatDF.filter(col("return_rate_category") === "Low Rate" && col("return_rate") < 5 && col("sale_price") < 50).show()

//  --------------------------------------------------------------------------------------------------------------------------------------
//    SQL Solution

    ecommerceReturn.createTempView("ecom")

    val ecomWithReturnRateCatDF = spark.sql(
      """
        | select *,
        | case
        |   when return_rate > 20 then 'High Rate'
        |   when return_rate between 10 and 20 then 'Moderate Rate'
        |   else 'Low Rate'
        | end as rate_category
        | from ecom
        |""".stripMargin
    )

    ecomWithReturnRateCatDF.show()

    ecomWithReturnRateCatDF.createTempView("ecom_2")

    spark.sql(
      """
        | select rate_category, count(product_name) as product_count
        | from ecom_2
        | group by rate_category
        |""".stripMargin).show()

    spark.sql(
      """
        | select rate_category, avg(return_rate) as avg_return_rate
        | from ecom_2
        | where rate_category = 'High Rate'
        | group by rate_category
        |""".stripMargin).show()

    spark.sql(
      """
        | select rate_category, max(return_rate) as max_return_rate
        | from ecom_2
        | where rate_category = 'Moderate Rate'
        | group by rate_category
        |""".stripMargin).show()

    spark.sql(
      """
        | select * from ecom_2
        | where
        | rate_category = 'Low Rate' and
        | return_rate < 5 and
        | sale_price < 50
        |""".stripMargin).show()



  }

}
