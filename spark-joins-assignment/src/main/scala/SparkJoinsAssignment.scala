import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast

object SparkJoinsAssignment {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark Joins Assignment")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // ==========================================================
    // DATA
    // ==========================================================

    val lookupData = Seq(
      ("US", "United States"),
      ("IN", "India"),
      ("UK", "United Kingdom")
    )

    val employeeData = Seq(
      ("Alice", "US"),
      ("Bob", "IN"),
      ("Cathy", "UK"),
      ("David", "US")
    )

    val lookupDF = spark.createDataFrame(lookupData)
      .toDF("CountryCode", "CountryName")

    val employeeDF = spark.createDataFrame(employeeData)
      .toDF("Name", "CountryCode")


    // ==========================================================
    // DISPLAY INPUT DATA
    // ==========================================================

    println("\n===== EMPLOYEE DATA =====")
    employeeDF.show()

    println("\n===== COUNTRY LOOKUP DATA =====")
    lookupDF.show()


    // ==========================================================
    // 1. BROADCAST HASH JOIN
    // ==========================================================

    println("\n===== 1. BROADCAST HASH JOIN =====")

    val broadcastJoinResult =
      employeeDF.join(
        broadcast(lookupDF),
        Seq("CountryCode"),
        "inner"
      )

    broadcastJoinResult.show()

    println("----- Broadcast Hash Join Execution Plan -----")
    broadcastJoinResult.explain(true)


    // ==========================================================
    // 2. SHUFFLE HASH JOIN
    // ==========================================================

    println("\n===== 2. SHUFFLE HASH JOIN =====")

    // Disable broadcasting so Spark does not choose Broadcast Join
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    val shuffleHashResult =
      employeeDF
        .hint("SHUFFLE_HASH")
        .join(
          lookupDF.hint("SHUFFLE_HASH"),
          Seq("CountryCode"),
          "inner"
        )

    shuffleHashResult.show()

    println("----- Shuffle Hash Join Execution Plan -----")
    shuffleHashResult.explain(true)


    // ==========================================================
    // 3. SHUFFLE SORT MERGE JOIN
    // ==========================================================

    println("\n===== 3. SHUFFLE SORT MERGE JOIN =====")

    val shuffleSortMergeResult =
      employeeDF
        .hint("MERGE")
        .join(
          lookupDF.hint("MERGE"),
          Seq("CountryCode"),
          "inner"
        )

    shuffleSortMergeResult.show()

    println("----- Shuffle Sort Merge Join Execution Plan -----")
    shuffleSortMergeResult.explain(true)


    // ==========================================================
    // 4. BROADCAST NESTED LOOP JOIN
    // ==========================================================

    println("\n===== 4. BROADCAST NESTED LOOP JOIN =====")

    // Cross Join creates combinations of every row
    val nestedLoopResult =
      employeeDF
        .hint("BROADCAST")
        .crossJoin(
          lookupDF.hint("BROADCAST")
        )

    nestedLoopResult.show()

    println("----- Broadcast Nested Loop Join Execution Plan -----")
    nestedLoopResult.explain(true)


    // ==========================================================
    // JOIN OPTIMIZATION
    // ==========================================================

    println("\n===== JOIN OPTIMIZATION TECHNIQUES =====")

    println("1. Broadcast small lookup tables.")
    println("2. Repartition large datasets using the join key.")
    println("3. Sort and bucket data on frequently used join keys.")
    println("4. Filter data before performing joins.")


    // ==========================================================
    // END
    // ==========================================================

    spark.stop()
  }
}
