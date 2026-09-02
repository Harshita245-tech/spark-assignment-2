import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object DataFrameAssignment {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark DataFrame Assignment")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    // ==========================================================
    // 1. SALES DATAFRAME
    // ==========================================================

    val salesData = Seq(
      ("C001", "Laptop", "Electronics", 75000.0, 2),
      ("C002", "Mobile", "Electronics", 30000.0, 5),
      ("C001", "Chair", "Furniture", 5000.0, 10),
      ("C003", "Table", "Furniture", 10000.0, 4),
      ("C002", "Headphones", "Electronics", 3000.0, 8)
    )

    val salesDF = salesData.toDF(
      "customer",
      "product",
      "category",
      "price",
      "quantity"
    )

    println("===== SALES DATA =====")
    salesDF.show()

    // ==========================================================
    // 2. TOTAL SALES
    // ==========================================================

    val totalSales = salesDF
      .withColumn(
        "total",
        col("price") * col("quantity")
      )

    println("===== TOTAL SALES =====")
    totalSales.show()

    // ==========================================================
    // 3. SIMPLE AGGREGATION
    // ==========================================================

    println("===== SIMPLE AGGREGATION =====")

    totalSales
      .agg(
        sum("total").alias("total_sales"),
        avg("price").alias("average_price"),
        max("price").alias("maximum_price"),
        min("price").alias("minimum_price")
      )
      .show()

    // ==========================================================
    // 4. GROUPING AGGREGATION BY CATEGORY
    // ==========================================================

    println("===== GROUPING AGGREGATION =====")

    totalSales
      .groupBy("category")
      .agg(
        sum("total").alias("total_sales"),
        avg("total").alias("average_sales"),
        count("*").alias("number_of_orders")
      )
      .orderBy(desc("total_sales"))
      .show()

    // ==========================================================
    // 5. GROUPING BY CUSTOMER
    // ==========================================================

    println("===== CUSTOMER AGGREGATION =====")

    totalSales
      .groupBy("customer")
      .agg(
        sum("total").alias("total_spending"),
        count("*").alias("number_of_orders")
      )
      .orderBy(desc("total_spending"))
      .show()

    // ==========================================================
    // 6. WINDOW AGGREGATION - RUNNING TOTAL
    // ==========================================================

    println("===== WINDOW RUNNING TOTAL =====")

    val windowSpec = Window
      .partitionBy("customer")
      .orderBy("product")
      .rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow
      )

    val runningTotal = totalSales
      .withColumn(
        "running_total",
        sum("total").over(windowSpec)
      )

    runningTotal.show()

    // ==========================================================
    // 7. WINDOW RANKING - ROW_NUMBER()
    // ==========================================================

    println("===== WINDOW RANKING =====")

    val rankingWindow = Window
      .partitionBy("customer")
      .orderBy(desc("total"))

    val rankedSales = totalSales
      .withColumn(
        "row_number",
        row_number().over(rankingWindow)
      )

    rankedSales.show()

    // ==========================================================
    // 8. CUSTOMER DATAFRAME
    // ==========================================================

    println("===== CUSTOMER DATAFRAME =====")

    val customerData = Seq(
      ("C001", "Harsha", "Hyderabad"),
      ("C002", "Ravi", "Bangalore"),
      ("C003", "Priya", "Chennai")
    )

    val customerDF = customerData.toDF(
      "customer",
      "customer_name",
      "city"
    )

    customerDF.show()

    // ==========================================================
    // 9. ORDERS DATAFRAME
    // ==========================================================

    println("===== ORDERS DATAFRAME =====")

    val ordersDF = totalSales.select(
      "customer",
      "product",
      "category",
      "price",
      "quantity",
      "total"
    )

    ordersDF.show()

    // ==========================================================
    // 10. SIMPLE INNER JOIN
    // ==========================================================

    println("===== SIMPLE INNER JOIN =====")

    val joinedDF = ordersDF
      .join(
        customerDF,
        Seq("customer"),
        "inner"
      )

    joinedDF.show()

    // ==========================================================
    // 11. AGGREGATION AFTER JOIN
    // ==========================================================

    println("===== AGGREGATION AFTER JOIN =====")

    val customerSales = joinedDF
      .groupBy(
        "customer",
        "customer_name",
        "city"
      )
      .agg(
        sum("total").alias("total_sales"),
        count("*").alias("number_of_orders")
      )
      .orderBy(desc("total_sales"))

    customerSales.show()

    // ==========================================================
    // 12. SHUFFLE SORT MERGE JOIN
    // ==========================================================

    println("===== SHUFFLE SORT MERGE JOIN =====")

    // Disable broadcast join so Spark can use
    // Shuffle Sort Merge Join.

    spark.conf.set(
      "spark.sql.autoBroadcastJoinThreshold",
      -1
    )

    spark.conf.set(
      "spark.sql.adaptive.enabled",
      "false"
    )

    val shuffleSortMergeJoin = ordersDF
      .join(
        customerDF,
        Seq("customer"),
        "inner"
      )

    shuffleSortMergeJoin.show()

    // ==========================================================
    // 13. EXECUTION PLAN
    // ==========================================================

    println("===== EXECUTION PLAN =====")

    shuffleSortMergeJoin.explain(true)

    // ==========================================================
    // 14. FINAL EXERCISE
    // JOIN + GROUPING + WINDOW RANKING
    // ==========================================================

    println("===== FINAL EXERCISE =====")

    // Step 1: JOIN
    val finalJoined = ordersDF
      .join(
        customerDF,
        Seq("customer"),
        "inner"
      )

    // Step 2: GROUPING
    val groupedSales = finalJoined
      .groupBy(
        "customer",
        "customer_name",
        "city",
        "category"
      )
      .agg(
        sum("total").alias("category_sales")
      )

    // Step 3: WINDOW RANKING
    val finalWindow = Window
      .partitionBy("customer")
      .orderBy(desc("category_sales"))

    val finalResult = groupedSales
      .withColumn(
        "rank",
        row_number().over(finalWindow)
      )
      .orderBy(
        "customer",
        "rank"
      )

    finalResult.show()

    // ==========================================================
    // STOP SPARK
    // ==========================================================

    spark.stop()
  }
}
