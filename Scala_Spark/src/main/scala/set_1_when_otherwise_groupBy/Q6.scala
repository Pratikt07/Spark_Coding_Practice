package set_1_when_otherwise_groupBy

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object Q6 {
  def main(args: Array[String]): Unit = {

    /*

    7. Vehicle Mileage Analysis
      Classify each vehicle’s mileage as "High Efficiency" if mileage is above 25 MPG, "Moderate Efficiency"
    if between 15-25 MPG, and "Low Efficiency" if below 15 MPG.
     Scala Spark Data

     */

    val spark = SparkSession.builder().appName("Q6").master("local[*]").getOrCreate()

    import spark.implicits._

    val vehicles = List(
      ("CarA", 30),
      ("CarB", 22),
      ("CarC", 18),
      ("CarD", 15),
      ("CarE", 10),
      ("CarF", 28),
      ("CarG", 12),
      ("CarH", 35),
      ("CarI", 25),
      ("CarJ", 16)
    ).toDF("vehicle_name", "mileage")

    vehicles.select(col("vehicle_name"), when(col("mileage") > 25, "High Efficiency")
    .when(col("mileage") < 25 && col("mileage") < 15, "Moderate Efficiency")
    .otherwise("Low Efficiency").alias("mileage_category")).show()

    vehicles.createTempView("v")

    spark.sql(
      """
        | Select
        |   vehicle_name,
        |   case
        |     when mileage > 25 then 'High Efficiency'
        |     when mileage > 15 and mileage < 25 then 'Moderate Efficiency'
        |     else 'Low Efficiency'
        |   end as mileage_category
        |  from v
        |""".stripMargin).show()

  }

}
