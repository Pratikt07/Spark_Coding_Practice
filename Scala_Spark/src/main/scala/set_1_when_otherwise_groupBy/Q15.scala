package set_1_when_otherwise_groupBy

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Q15 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Q15").master("local[*]").getOrCreate()

    import spark.implicits._

    /*
      Scenario 16: Electricity Consumption and Rate Assignment

      Question Set:
      1. Classify households into "High Usage" if kWh exceeds 500 and bill exceeds $200,
      "Medium Usage" for kWh between 200-500 and bill between $100-$200, and "Low Usage"
      otherwise. Calculate the total number of households in each usage category.
      2. Find the maximum bill amount for "High Usage" households and calculate the average kWh
      for "Medium Usage" households.
      3. Identify households with "Low Usage" but kWh usage exceeding 300. Count such
      households.

     */

    val electricityUsage = List(
      ("House1", 550, 250),
      ("House2", 400, 180),
      ("House3", 150, 50),
      ("House4", 500, 200),
      ("House5", 600, 220),
      ("House6", 350, 120),
      ("House7", 100, 30),
      ("House8", 480, 190),
      ("House9", 220, 105),
      ("House10", 150, 60),
      ("House11", 320, 70)
    ).toDF("household", "kwh_usage", "total_bill")

    val electricUsageCategory = electricityUsage.withColumn("usage_category", when(col("kwh_usage") > 500 && col("total_bill") > 200, "High Usage")
                                                            .when(col("kwh_usage").between(200,500) && col("total_bill").between(100,200), "Medium Usage")
                                                            .otherwise("Low Usage"))
    electricUsageCategory.show()

    electricUsageCategory.groupBy(col("usage_category")).agg(count(col("household")).alias("no_of_household")).show()

    electricUsageCategory.groupBy(col("usage_category")).agg(max(col("total_bill")).alias("max_bill_amount")).filter(col("usage_category") === "High Usage").show()

    electricUsageCategory.groupBy(col("usage_category")).agg(avg(col("kwh_usage")).alias("avg_kwh_usage")).filter(col("usage_category") === "Medium Usage").show()

    electricUsageCategory.filter(col("kwh_usage") > 300 && col("usage_category") === "Low Usage").groupBy("usage_category").agg(count(col("household")).alias("count")).show()

//    ------------------------------------------------------------------------------------------------
//    SQL Solutions

    electricityUsage.createTempView("electric_usage")

    val sqlElectricUsageCatDF = spark.sql(
      """
        | select *,
        | case
        |   when kwh_usage > 500 and total_bill > 200 then 'High Usage'
        |   when ( kwh_usage between 200 and 500 ) and ( total_bill between 100 and 200) then 'Medium Usage'
        |   else 'Low Usage'
        | end as usage_category
        | from electric_usage
        |""".stripMargin)

    sqlElectricUsageCatDF.createTempView("electric_usg_cat")

    spark.sql(
      """
        | select usage_category, count(household) as household_count
        | from  electric_usg_cat
        | group by usage_category
        |""".stripMargin).show()

    spark.sql(
      """
        | select usage_category, max(total_bill) as max_bill_amount
        | from  electric_usg_cat
        | group by usage_category
        | having usage_category = 'High Usage'
        |""".stripMargin).show()

    spark.sql(
      """
        | select usage_category, avg(kwh_usage) as avg_kwh_usage
        | from  electric_usg_cat
        | group by usage_category
        | having usage_category = 'Medium Usage'
        |""".stripMargin).show()

    spark.sql(
      """
        | with low_usg_cat as ( select * from electric_usg_cat where usage_category = 'Low Usage' and kwh_usage > 300 )
        | select usage_category, count(household) as number_of_household
        | from  low_usg_cat
        | group by usage_category
        |""".stripMargin).show()


  }

}
