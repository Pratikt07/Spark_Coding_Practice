package set_1_when_otherwise_groupBy

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Q12 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Q12").master("local[*]").getOrCreate()

    import spark.implicits._

    /*
      13. E-commerce Order Fulfillment Timeliness Based on Product Type and Location

      Classify orders as "Delayed" if delivery time exceeds 7 days and origin location is "International",
      "On-Time" if between 3-7 days, and "Fast" if below 3 days. Group by product type to see the count of
      each delivery speed category.

      Expected Output: Orders categorized by delivery speed, showing the number of each type per product

     */

    val orders = List(
      ("Order1", "Laptop", "Domestic", 2),
      ("Order2", "Shoes", "International", 8),
      ("Order3", "Shoes", "Domestic", 3),
      ("Order4", "Smartphone", "Domestic", 3),
      ("Order5", "Tablet", "International", 5),
      ("Order6", "Watch", "Domestic", 7),
      ("Order7", "Headphones", "International", 10),
      ("Order8", "Camera", "Domestic", 1),
      ("Order9", "Shoes", "International", 9),
      ("Order10", "Laptop", "Domestic", 6),
      ("Order11", "Tablet", "International", 4)
    ).toDF("order_id", "product_type", "origin", "delivery_days")

    val order_with_category_df = orders.select(col("*"), when(col("delivery_days") > 7 && col("origin") === "International", "Delayed" )
                            .when(col("delivery_days").between(3,7), "On-Time" )
                            .otherwise("Fast").alias("delivery_speed_category") )

    order_with_category_df.show()

    order_with_category_df.groupBy(col("product_type"), col("delivery_speed_category")).agg(count(col("delivery_speed_category")).alias("delivery_speed_category_count")).show()


    orders.createTempView("orders")

    // productType count
    val sql_orders_1 = spark.sql(
      """
        | select *,
        | case
        |   when delivery_days > 7 and origin = 'International' then 'Delayed'
        |   when delivery_days between 3 and 7 then 'On-Time'
        |   else 'Fast'
        | end as delivery_speed_category
        | from orders
        |""".stripMargin)

    sql_orders_1.show()

    sql_orders_1.createTempView("orders_2")

    // delivery_speed_category count
    spark.sql(
      """
        | select
        | product_type,
        | delivery_speed_category,
        | count(delivery_speed_category) as delivery_speed_category_count
        | from orders_2
        | group by product_type, delivery_speed_category
        |""".stripMargin).show()


  }

}
