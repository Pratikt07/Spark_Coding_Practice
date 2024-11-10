package set_1_when_otherwise_groupBy

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Q21 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Q21").master("local[*]").getOrCreate()

    import spark.implicits._
    /*

        Scenario 22: Banking Fraud Detection
        Question Set:
          1. Classify transactions as "High Risk" if the transaction amount is above 10,000 and frequency
          of transactions from the same account within a day exceeds 5, "Moderate Risk" if the
          amount is between 5,000 and 10,000 and frequency is between 2 and 5, and "Low Risk"
          otherwise. Calculate the total number of transactions in each risk level.
          2. Identify accounts with at least one "High Risk" transaction and the total amount transacted
          by those accounts.
          3. Find all "Moderate Risk" transactions where the account type is "Savings" and the amount is
          above 7,500.
    */

    val transactions = List(
      ("Account1", "2024-11-01", 12000, 6, "Savings"),
      ("Account1", "2024-11-10", 400000, 9, "Savings"),
      ("Account2", "2024-11-01", 8000, 3, "Current"),
      ("Account3", "2024-11-02", 2000, 1, "Savings"),
      ("Account4", "2024-11-02", 15000, 7, "Savings"),
      ("Account5", "2024-11-03", 9000, 4, "Current"),
      ("Account6", "2024-11-03", 3000, 1, "Current"),
      ("Account6", "2024-11-03", 22000, 6, "Current"),
      ("Account7", "2024-11-04", 13000, 5, "Savings"),
      ("Account8", "2024-11-04", 6000, 2, "Current"),
      ("Account9", "2024-11-05", 20000, 8, "Savings"),
      ("Account10", "2024-11-05", 7000, 3, "Savings"),
      ("Account10", "2024-11-06", 1000, 2, "Savings")
    ).toDF("account_id", "transaction_date", "amount", "frequency", "account_type")

    val transactionsCategory = transactions.withColumn("transaction_category", when(col("amount") > 10000 && col("frequency") > 5, "High Risk")
      .when(col("amount").between(5000, 10000) && col("frequency").between(2, 5), "Moderate Risk").otherwise("Low Risk"))

    transactionsCategory.groupBy(col("transaction_category")).agg(count(col("account_id")).alias("No_Of_Transaction")).show()

    transactionsCategory.groupBy(col("account_id"), col("transaction_category"))
      .agg(count(col("transaction_category")).alias("category_count"), sum(col("amount")).alias("total_amount"))
      .filter(col("transaction_category") === "High Risk")
      .orderBy(col("transaction_category")).show()

    transactionsCategory.filter(col("transaction_category") === "Low Risk" && col("account_type") === "Savings" && col("amount") > 7500).show()
    //---------------------------------------------------------------------------------------------------------------------------------------------
    //    SQL Solution

    transactions.createTempView("transactions")

    val transactionCat = spark.sql(
      """
   SELECT *,
         CASE
             WHEN amount > 10000 AND frequency > 5 THEN 'High Risk'
             WHEN amount BETWEEN 5000 AND 10000 AND frequency BETWEEN 2 AND 5 THEN 'Moderate Risk'
             ELSE 'Low Risk'
         END AS transaction_category
  FROM transactions
""")

    transactionCat.createTempView("transactionsCategory")

    val result1 = spark.sql(
      """
  SELECT transaction_category, COUNT(account_id) AS No_Of_Transaction
  FROM transactionsCategory
  GROUP BY transaction_category
""")

    result1.show()

    val result2 = spark.sql(
      """
  SELECT account_id, transaction_category,
         COUNT(transaction_category) AS category_count,
         SUM(amount) AS total_amount
  FROM transactionsCategory
  WHERE transaction_category = 'High Risk'
  GROUP BY account_id, transaction_category
  ORDER BY transaction_category
""")

    result2.show()

    val result3 = spark.sql(
      """
  SELECT *
  FROM transactionsCategory
  WHERE transaction_category = 'Low Risk'
    AND account_type = 'Savings'
    AND amount > 7500
""")

    result3.show()


  }

}
