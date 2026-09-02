import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StatefulWordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("StatefulWordCount")
      .setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))

    // Required for stateful operations
    ssc.checkpoint("checkpoint")

    // Connect to TCP producer
    val lines = ssc.socketTextStream(
      "localhost",
      9999
    )

    // Split lines into words
    val words = lines.flatMap(
      line => line.split("\\s+")
    )

    // Convert to (word, 1)
    val pairs = words.map(
      word => (word.toLowerCase, 1)
    )

    // Function to maintain running counts
    val updateFunc = (
      newValues: Seq[Int],
      runningCount: Option[Int]
    ) => {
      Some(
        newValues.sum + runningCount.getOrElse(0)
      )
    }

    // Maintain state across batches
    val runningCounts = pairs.updateStateByKey(updateFunc)

    // Display running counts
    runningCounts.print()

    // Start streaming
    ssc.start()

    // Wait for termination
    ssc.awaitTermination()
  }
}
