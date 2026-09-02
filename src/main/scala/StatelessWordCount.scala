import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StatelessWordCount {

  def main(args: Array[String]): Unit = {

    // 1. Spark configuration
    val conf = new SparkConf()
      .setAppName("StatelessWordCount")
      .setMaster("local[*]")

    // 2. Create StreamingContext
    val ssc = new StreamingContext(conf, Seconds(5))

    // 3. Connect to TCP producer
    val lines = ssc.socketTextStream(
      "localhost",
      9999
    )

    // 4. Split lines into words
    val words = lines.flatMap(
      line => line.split("\\s+")
    )

    // 5. Convert words to (word, 1)
    val pairs = words.map(
      word => (word.toLowerCase, 1)
    )

    // 6. Count words
    val wordCounts = pairs.reduceByKey(_ + _)

    // 7. Display result
    wordCounts.print()

    // 8. Start streaming
    ssc.start()

    // 9. Wait for termination
    ssc.awaitTermination()
  }
}
