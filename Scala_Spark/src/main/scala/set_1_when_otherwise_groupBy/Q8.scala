package set_1_when_otherwise_groupBy

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, sum, when}

object Q8 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Q8").master("local[*]").getOrCreate()

    import spark.implicits._
    /*
    9. Product Inventory Check
      Classify inventory stock levels as "Overstocked" if stock exceeds 100, "Normal" if between 50-100,
    and "Low Stock" if below 50. Aggregate total stock in each category.
     Scala Spark Data0
    */
    val inventory = List(
      ("ProductA", 120),
      ("ProductB", 95),
      ("ProductC", 45),
      ("ProductD", 200),
      ("ProductE", 75),
      ("ProductF", 30),
      ("ProductG", 85),
      ("ProductH", 100),
      ("ProductI", 60),
      ("ProductJ", 20)
    ).toDF("product_name", "stock_quantity")

    val stock_level_df = inventory.select(col("product_name"), col("stock_quantity"), when(col("stock_quantity") > 100, "Overstocked")
              .when(col("stock_quantity") < 50, "Low Stock").otherwise("Normal").alias("stock_level")  )
    stock_level_df.show()

    stock_level_df.groupBy(col("stock_level")).agg(sum(col("stock_quantity")).alias("total_stock")).show()

    inventory.createTempView("inv")

    val inv_stock_level_df = spark.sql(
      """
        | select product_name, stock_quantity,
        | case
        |   when stock_quantity > 100 then 'Overstocked'
        |   when stock_quantity < 50 then 'Low Stock'
        |   else 'Normal'
        | end as stock_level
        | from inv
        |""".stripMargin)

    inv_stock_level_df.show()

    inv_stock_level_df.createTempView("inv_with_level")

    spark.sql(
      """
        | select stock_level,
        | sum(stock_quantity) as total_stock
        | from inv_with_level
        | group by stock_level
        |""".stripMargin).show()
  }

}
