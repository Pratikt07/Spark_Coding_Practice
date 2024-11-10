package set_1_when_otherwise_groupBy

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Q17 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Q17").master("local[*]").getOrCreate()

    import spark.implicits._

    /*
     Scenario 18: Product Sales Analysis
      Question Set:
      1. Classify products as "Top Seller" if total sales exceed 200,000 and discount offered is less
      than 10%, "Moderate Seller" if total sales are between 100,000 and 200,000, and "Low
      Seller" otherwise. Count the total number of products in each classification.
      2. Find the maximum sales value among "Top Seller" products and the minimum discount rate
      among "Moderate Seller" products.
      3. Identify products from the "Low Seller" category with a total sales value below 50,000 and
      discount offered above 15%.
    */
    val productSales = List(
      ("Product1", 250000, 5),
      ("Product2", 150000, 8),
      ("Product3", 50000, 20),
      ("Product4", 120000, 10),
      ("Product5", 300000, 7),
      ("Product6", 60000, 18),
      ("Product7", 180000, 9),
      ("Product8", 45000, 25),
      ("Product9", 70000, 15),
      ("Product10", 10000, 30)
    ).toDF("product_name", "total_sales", "discount")

    val productCatDF = productSales.withColumn("product_category", when(col("total_sales") > 200000 && col("discount") < 10, "Top Seller")
      .when(col("total_sales").between(100000, 200000), "Moderate Seller")
      .otherwise("Low Seller"))

    productCatDF.groupBy(col("product_category")).agg(count(col("product_name")).alias("Product_Count")).show()

    productCatDF.groupBy(col("product_category")).agg(max(col("total_sales")).alias("Max_Sales")).filter(col("product_category") === "Top Seller").show()

    productCatDF.groupBy(col("product_category")).agg(min(col("discount")).alias("Min_Discount")).filter(col("product_category") === "Moderate Seller").show()

    productCatDF.filter(col("product_category").equalTo("Low Seller") && col("total_sales") < 50000 && col("discount") > 15).show()

//    ---------------------------------------------------------------------------------------------------------------------------------
//    SQL Solution

    productSales.createTempView("product_sales")

    val productSalesCatDF = spark.sql(
      """
        | select *,
        | case
        |   when total_sales > 200000 and discount < 10 then 'Top Seller'
        |   when total_sales between 100000 and 200000 then 'Moderate Seller'
        |   else 'Low Seller'
        | end as seller_category
        | from product_sales
        |""".stripMargin)

    productSalesCatDF.show()

    productSalesCatDF.createTempView("product_sales_cat")

    spark.sql(
      """
        | select seller_category, count(product_name) as product_count from product_sales_cat group by seller_category
        |""".stripMargin).show()

    spark.sql(
      """
        | select seller_category, max(total_sales) as max_total_sales from product_sales_cat where seller_category = 'Top Seller' group by seller_category
        |""".stripMargin).show()

    spark.sql(
      """
        | select seller_category, min(discount) as min_discount from product_sales_cat where seller_category = 'Moderate Seller' group by seller_category
        |""".stripMargin).show()

    spark.sql(
      """
        | select * from product_sales_cat where seller_category = 'Low Seller' and total_sales > 50000 and discount > 15
        |""".stripMargin).show()

  }
}
