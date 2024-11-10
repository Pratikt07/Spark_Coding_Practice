package set_1_when_otherwise_groupBy

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object Q14 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Q14").master("local[*]").getOrCreate()

    import spark.implicits._

  /*

    Scenario 15: Customer Purchase Recency Categorization

    Question Set: 4. Categorize customers based on purchase recency: "Frequent" if last purchase within
    30 days, "Occasional" if within 60 days, and "Rare" if over 60 days. Show the number of each
    category per membership type.

    5. Find the average total purchase amount for customers with "Frequent" purchase recency
    and "Premium" membership.

    6. For customers with "Rare" recency, calculate the minimum purchase amount across different
    membership types.

   */

    val customerPurchases = List(
      ("karthik", "Premium", 50, 5000),
      ("neha", "Standard", 10, 2000),
      ("priya", "Premium", 65, 8000),
      ("mohan", "Basic", 90, 1200),
      ("ajay", "Standard", 25, 3500),
      ("vijay", "Premium", 15, 7000),
      ("veer", "Basic", 75, 1500),
      ("aatish", "Standard", 45, 3000),
      ("animesh", "Premium", 20, 9000),
      ("nishad", "Basic", 80, 1100)
    ).toDF("name", "membership", "days_since_last_purchase", "total_purchase_amount")

    val custPurchaseRecency = customerPurchases.withColumn("Purchase_Recency",when(col("days_since_last_purchase") < 30, "Frequent").when(col("days_since_last_purchase") < 60, "Occasional").otherwise("Rare") )

    custPurchaseRecency.show()

    custPurchaseRecency.groupBy(col("membership"), col("Purchase_Recency")).agg(count(col("Purchase_Recency")).alias("Purchase_Recency_Count")).show()


    custPurchaseRecency.groupBy(col("membership"), col("Purchase_Recency")).agg(avg(col("total_purchase_amount")).alias("Average_Purchase_Amount")).filter(col("membership") === "Premium" && col("Purchase_Recency")  === "Frequent").show()

    custPurchaseRecency.groupBy(col("membership"), col("Purchase_Recency")).agg(min(col("total_purchase_amount")).alias("Min_Total_Purchase_Amount")).filter( col("Purchase_Recency")  === "Rare").show()

//    ----------------------------------------------------------------------------------------------------------
//    SQL solutions

    customerPurchases.createTempView("cust_purchase")

    val sqlDFCustPurchaseRecency = spark.sql(
      """
        | select *,
        | case
        |   when days_since_last_purchase < 30 then 'Frequent'
        |   when days_since_last_purchase < 60 then 'Occasional'
        |   else 'Rare'
        | end as purchase_recency
        | from cust_purchase
        |""".stripMargin)

    sqlDFCustPurchaseRecency.show()

    sqlDFCustPurchaseRecency.createTempView("cust_recency")

    spark.sql(
      """
        | select membership, purchase_recency, count(purchase_recency)
        |   from cust_recency
        |   group by membership, purchase_recency
        |""".stripMargin).show()

    spark.sql(
      """
        | select membership, purchase_recency, avg(total_purchase_amount)
        |   from cust_recency
        |   group by membership, purchase_recency
        |   having membership = 'Premium' and purchase_recency = 'Frequent'
        |""".stripMargin).show()

    spark.sql(
      """
        | select membership, purchase_recency, min(total_purchase_amount)
        |   from cust_recency
        |   group by membership, purchase_recency
        |   having purchase_recency = 'Rare'
        |""".stripMargin).show()





  }
}
