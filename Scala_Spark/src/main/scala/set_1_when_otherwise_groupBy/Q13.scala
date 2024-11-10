package set_1_when_otherwise_groupBy

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Q13 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Q13").master("local[*]").getOrCreate()

    import spark.implicits._

    /*

    Scenario 14: Financial Risk Level Classification for Loan Applicants

    Question Set:
    1. Classify loan applicants as "High Risk" if the loan amount exceeds twice their income and
    credit score is below 600, "Moderate Risk" if the loan amount is between 1-2 times their
    income and credit score between 600-700, and "Low Risk" otherwise. Find the total count of
    each risk level.

    2. For applicants classified as "High Risk," calculate the average loan amount by income range
    (e.g., < 50k, 50-100k, >100k).

    3. Group by income brackets (<50k, 50-100k, >100k) and calculate the average credit score for
    each risk level. Filter for groups where average credit score is below 650.

     */

    val loanApplicants = List(
      ("karthik", 60000, 120000, 590),
      ("neha", 90000, 180000, 610),
      ("priya", 50000, 75000, 680),
      ("mohan", 120000, 240000, 560),
      ("ajay", 45000, 60000, 620),
      ("vijay", 100000, 100000, 700),
      ("veer", 30000, 90000, 580),
      ("aatish", 85000, 85000, 710),
      ("animesh", 50000, 100000, 650),
      ("nishad", 75000, 200000, 540)
    ).toDF("name", "income", "loan_amount", "credit_score")

    val Q1_1_DF = loanApplicants.select(col("*"),
      col("income").multiply(2).alias("twice_income"),
      when(col("loan_amount") > ( col("income").multiply(2)) && col("credit_score") < 600, "High Risk")
      .when(col("loan_amount").between(col("income").multiply(1), col("income").multiply(2) ) && col("credit_score").between(600, 700), "Moderate Risk")
      .otherwise("Low Risk").alias("risk_category") )

    Q1_1_DF.show()

    val Q1_2_DF = Q1_1_DF.groupBy(col("risk_category")).agg(count(col("risk_category")).alias("risk_category_count"))

//    Q1_2_DF.show()

    val Q2_DF_1 = Q1_1_DF.select(col("*"),
      when(col("income") < 50000, "< 50k").when(col("income").between(50000, 100000),"50k - 100k").otherwise("> 100k").alias("income_range")  )

    Q2_DF_1.show()

    val Q2_DF_2 = Q2_DF_1.filter(col("risk_category") === "High Risk").groupBy(col("income_range")).agg(avg(col("loan_amount")).alias("Average loan Amount By income range"))

//    Q2_DF_2.show()

    val Q3_DF = Q2_DF_1.groupBy(col("income_range"), col("risk_category")).agg(avg(col("credit_score")).alias("avg_score"))//.filter(col("avg_score") < 600)
//    Q3_DF.show()
//
//    SQL Solutions
    loanApplicants.createTempView("loanApp")

    val Q1_1_SQL = spark.sql(
      """
        | select *,
        | case
        |   when loan_amount > ( 2 * income ) and credit_score < 600 then 'High Risk'
        |   when ( loan_amount between income and  ( 2 * income ) ) and ( credit_score between 600 and 700 ) then 'Moderate Risk'
        |   else 'Low Risk'
        | end as risk_category
        | from loanApp
        |""".stripMargin)
    Q1_1_SQL.show()

    Q1_1_SQL.createTempView("loanApp_1_2")

    spark.sql(
      """
        | select risk_category,
        | count(risk_category)
        | from loanApp_1_2
        | group by risk_category
        |""".stripMargin)//.show()

    val Q2_SQL_1 = spark.sql(
      """
        | select *,
        | case
        |   when income < 50000 then '< 50k'
        |   when income >= 50000 and income <= 100000 then '50k-100k'
        |   else '> 100k'
        | end as income_range
        | from loanApp_1_2
        |""".stripMargin)

    Q2_SQL_1.show()

    Q2_SQL_1.createTempView("Q2_1")

    spark.sql(
      """
        | select
        |   income_range,
        |   avg(loan_amount) as Average_Loan_Amount
        | from Q2_1
        | where risk_category = 'High Risk'
        | group by income_range
        |""".stripMargin)

    spark.sql(
      """
        | select
        |   income_range,
        |   risk_category,
        |   avg(credit_score) as avg_score
        | from Q2_1
        | group by income_range, risk_category
        |""".stripMargin).show()

    Q3_DF.show()


  }

}
