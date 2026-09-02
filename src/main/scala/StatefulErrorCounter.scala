import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StatefulErrorCounter {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("StatefulErrorCounter")
      .setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))

    // Required for stateful operations
    ssc.checkpoint("checkpoint")

    // Connect to TCP producer
    val lines = ssc.socketTextStream(
      "localhost",
      9999
    )

    // Filter ERROR messages
    val errors = lines.filter(
      line => line.contains("ERROR")
    )

    // Convert each ERROR to (ERROR, 1)
    val errorPairs = errors.map(
      _ => ("ERROR", 1)
    )

    // Function to maintain running error count
    val updateFunc = (
      newValues: Seq[Int],
      runningCount: Option[Int]
    ) => {
      Some(
        newValues.sum + runningCount.getOrElse(0)
      )
    }

    // Maintain cumulative error count
    val totalErrors = errorPairs.updateStateByKey(updateFunc)

    // Display running error count
    totalErrors.print()

    // Start streaming
    ssc.start()

    // Wait for termination
    ssc.awaitTermination()
  }
}
