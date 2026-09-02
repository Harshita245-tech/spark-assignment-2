# Spark Streaming DStreams Assignment

## Overview

This project demonstrates real-time data processing using **Apache Spark Streaming and DStreams with Scala**.

The assignment focuses on:

- Producer–Consumer architecture
- Real-time stream processing
- DStreams
- Micro-batch processing
- Stateless Word Count
- Stateful Word Count
- Stateful Error/Log Monitoring
- Netcat as a real-time data producer
- Spark Streaming as the consumer

The complete flow is:

```text
                 PRODUCER
                    |
                    | TCP messages
                    v
              Netcat :9999
                    |
                    v
             Spark Streaming
                    |
                   DStream
                    |
          +---------+---------+
          |                   |
          v                   v
     Stateless            Stateful
     Processing           Processing
          |                   |
          v                   v
   Word Count per       Running Word Count
      Batch             / Error Count
```

---

# 1. Technologies Used

- Apache Spark
- Spark Streaming
- DStreams
- Scala
- SBT
- Netcat
- Java
- Linux/Ubuntu
- Git and GitHub

---

# 2. Producer–Consumer Concept

## Producer

A **producer** is a component that generates or sends data.

In this assignment, we use **Netcat (`nc`) as the producer**.

We start Netcat using:

```bash
nc -lk 9999
```

The producer listens on:

```text
localhost:9999
```

We can then type messages such as:

```text
hello spark
hello scala
spark spark scala
```

These messages are sent to the Spark Streaming application.

---

## Consumer

The **consumer** receives and processes the data produced by the producer.

In our assignment, **Spark Streaming acts as the consumer**.

The Spark application connects to:

```text
localhost:9999
```

using:

```scala
ssc.socketTextStream("localhost", 9999)
```

Therefore:

```text
Netcat
  |
  | sends messages
  v
Spark Streaming
  |
  | processes messages
  v
DStream
```

---

# 3. Two-Terminal Setup

We used two terminals to demonstrate the Producer–Consumer architecture.

## Terminal 1 — Producer

Run:

```bash
nc -lk 9999
```

Then enter messages:

```text
hello spark
hello scala
spark spark scala
```

Terminal 1 is responsible for **sending the input data**.

---

## Terminal 2 — Consumer

Run one of the Spark applications:

```bash
sbt "runMain StatelessWordCount"
```

or:

```bash
sbt "runMain StatefulWordCount"
```

or:

```bash
sbt "runMain StatefulErrorCounter"
```

Terminal 2 is responsible for **receiving and processing the input using Spark Streaming**.

---

# 4. What is a DStream?

A **DStream (Discretized Stream)** represents a continuous stream of data divided into small batches.

Conceptually:

```text
Continuous Data
       |
       v
+-------------+
|   Batch 1   |
+-------------+
       |
+-------------+
|   Batch 2   |
+-------------+
       |
+-------------+
|   Batch 3   |
+-------------+
```

Each batch is processed by Spark as an RDD.

The DStream is created using:

```scala
val lines = ssc.socketTextStream(
  "localhost",
  9999
)
```

---

# 5. Micro-Batch Processing

We used a **5-second batch interval**:

```scala
val ssc = new StreamingContext(conf, Seconds(5))
```

This means Spark checks and processes incoming data approximately every 5 seconds.

For example:

```text
0 sec
 |
5 sec  ---> Batch 1
 |
10 sec ---> Batch 2
 |
15 sec ---> Batch 3
 |
20 sec ---> Batch 4
```

Therefore, the input is processed as a sequence of small batches instead of processing everything as one large dataset.

---

# 6. Project Structure

```text
spark-assignment-2/
│
├── build.sbt
├── .gitignore
├── README.md
│
├── project/
│   └── build.properties
│
└── src/
    └── main/
        ├── resources/
        │   └── log4j2.properties
        │
        └── scala/
            ├── StatelessWordCount.scala
            ├── StatefulWordCount.scala
            └── StatefulErrorCounter.scala
```

---

# 7. Stateless Word Count

File:

```text
src/main/scala/StatelessWordCount.scala
```

The Stateless Word Count processes every batch independently.

The main operations are:

```scala
flatMap
map
reduceByKey
```

The important counting operation is:

```scala
val wordCounts = pairs.reduceByKey(_ + _)
```

---

## Stateless Example

### Input Batch 1

```text
apple orange apple
```

Output:

```text
(apple,2)
(orange,1)
```

### Input Batch 2

```text
apple banana
```

Output:

```text
(apple,1)
(banana,1)
```

Notice that the second batch gives:

```text
apple -> 1
```

It does **not** add the previous count.

This is because Stateless processing does not remember previous batches.

---

## Our Actual Stateless Test

We used Netcat to send:

```text
apple
orange
apple
```

and then:

```text
apple
banana
```

Spark produced results such as:

```text
(apple,1)
(orange,1)
(apple,1)
```

and in the next batch:

```text
(apple,1)
(banana,1)
```

This demonstrates that each batch is processed independently.

---

# 8. Stateful Word Count

File:

```text
src/main/scala/StatefulWordCount.scala
```

Stateful Word Count maintains information from previous batches.

The key operation is:

```scala
updateStateByKey
```

The application uses:

```scala
ssc.checkpoint("checkpoint")
```

Checkpointing is used to support stateful processing.

The state update function is responsible for combining the current batch count with the previous state.

---

# 9. How Stateful Word Count Works

Suppose the first batch contains:

```text
hello spark
```

The result is:

```text
hello -> 1
spark -> 1
```

Then the next batch contains:

```text
hello scala
```

The previous state is remembered.

The running result becomes:

```text
hello -> 2
spark -> 1
scala -> 1
```

If another batch contains:

```text
hello spark
```

the running result becomes:

```text
hello -> 3
spark -> 2
scala -> 1
```

This is the main difference between Stateful and Stateless processing.

---

# 10. Stateful Word Count Output

During our test, we sent messages through Netcat and Spark Streaming displayed running word counts.

Example:

```text
(spark,1)
(hello,1)
```

After additional input, the counts increased across batches.

For example:

```text
(spark,2)
(scala,1)
(hello,2)
```

The previous batch information was retained.

Therefore, Stateful Word Count provides a **running/cumulative count**.

---

# 11. Stateful Error Counter / Log Analysis

File:

```text
src/main/scala/StatefulErrorCounter.scala
```

This application performs real-time **log analysis**.

It reads incoming log messages and looks specifically for:

```text
ERROR
```

The application filters the incoming stream using:

```scala
.filter(_.contains("ERROR"))
```

Then each error is converted into:

```text
("ERROR", 1)
```

The stateful operation:

```scala
updateStateByKey
```

maintains the total number of errors across batches.

---

# 12. Real-Time Log Analysis

Example producer input:

```text
INFO login
ERROR database
ERROR payment
```

Spark filters the messages and ignores the INFO message.

The ERROR messages are counted.

Output:

```text
(ERROR,2)
```

If another error is received:

```text
ERROR database
```

the running result becomes:

```text
(ERROR,3)
```

If two more errors arrive:

```text
ERROR payment
ERROR API
```

the running result becomes:

```text
(ERROR,5)
```

Therefore, the application can continuously monitor the number of ERROR messages received.

---

# 13. Our Error Monitoring Test

We used Netcat as the producer and entered:

```text
INFO login
ERROR database
ERROR payment
```

Spark processed the stream and produced:

```text
(ERROR,1)
```

and then:

```text
(ERROR,2)
```

The output shows the cumulative number of ERROR messages.

This demonstrates real-time log/error monitoring using Spark Streaming.

---

# 14. Stateless vs Stateful

| Feature | Stateless | Stateful |
|---|---|---|
| Remembers previous batches | No | Yes |
| Processes batches independently | Yes | No |
| Running count | No | Yes |
| Main operation | `reduceByKey` | `updateStateByKey` |
| Previous state required | No | Yes |
| Checkpoint generally needed | No | Yes |
| Example | Word count per batch | Running word/error count |

### Simple explanation

**Stateless:**

```text
Batch 1 -> process
Batch 2 -> process separately
Batch 3 -> process separately
```

**Stateful:**

```text
Batch 1 -> process -> remember
Batch 2 -> previous state + new data
Batch 3 -> previous state + new data
```

---

# 15. Important Spark Operations Used

## `flatMap`

Used to split each input line into individual words.

Example:

```text
hello spark
```

becomes:

```text
hello
spark
```

---

## `map`

Used to convert each word into a key-value pair:

```text
hello -> (hello,1)
spark -> (spark,1)
```

---

## `reduceByKey`

Used in Stateless Word Count to combine values with the same key.

Example:

```text
apple -> 1
apple -> 1
```

becomes:

```text
apple -> 2
```

---

## `filter`

Used in Error Counter to select only ERROR messages.

Example:

```text
INFO login
ERROR database
INFO logout
ERROR payment
```

After filtering:

```text
ERROR database
ERROR payment
```

---

## `updateStateByKey`

Used for Stateful processing.

It combines the current batch's values with the previous state.

This allows us to maintain a running count.

---

# 16. Commands Used

## Check Netcat

```bash
nc -h
```

## Start Netcat Producer

```bash
nc -lk 9999
```

## Compile the project

```bash
sbt compile
```

## Run Stateless Word Count

```bash
sbt "runMain StatelessWordCount"
```

## Run Stateful Word Count

```bash
sbt "runMain StatefulWordCount"
```

## Run Stateful Error Counter

```bash
sbt "runMain StatefulErrorCounter"
```

---

# 17. Assignment Exercises Completed

## Lab Exercise 1 — Stateless Word Count

Input:

```text
apple orange apple
```

Expected count:

```text
apple -> 2
orange -> 1
```

Next batch:

```text
apple banana
```

Expected:

```text
apple -> 1
banana -> 1
```

The second batch does not remember the first batch.

---

## Lab Exercise 2 — Stateful Word Count

Input Batch 1:

```text
apple orange apple
```

Running result:

```text
apple -> 2
orange -> 1
```

Input Batch 2:

```text
apple banana
```

Running result:

```text
apple -> 3
orange -> 1
banana -> 1
```

Input Batch 3:

```text
orange apple
```

Running result:

```text
apple -> 4
orange -> 2
banana -> 1
```

---

## Lab Exercise 3 — Real-Time Log Monitoring

Example producer input:

```text
INFO Application started
INFO User login
ERROR Database unavailable
INFO User logout
ERROR Payment failed
```

The application:

1. Reads the stream
2. Filters ERROR messages
3. Counts errors
4. Maintains a running error count

Architecture:

```text
Netcat
   |
   v
DStream
   |
   v
filter(ERROR)
   |
   v
map(ERROR,1)
   |
   v
updateStateByKey
   |
   v
Running Error Count
```

---

# 18. Output Evidence

Screenshots of the successful execution outputs are also included in this GitHub repository.

The screenshots demonstrate:

- Netcat producer input
- Stateless Word Count output
- Stateful Word Count output
- Stateful Error Counter output
- Real-time batch processing
- Running/cumulative counts

The screenshots are provided as **execution evidence** along with the Scala source code.

---

# 19. What We Achieved

Through this assignment, we implemented a complete local real-time streaming pipeline:

```text
                 Input
                   |
                   v
              Netcat Producer
                   |
                   | TCP :9999
                   v
           Spark Streaming
                   |
                   v
                DStream
                   |
          +--------+--------+
          |                 |
          v                 v
      Stateless          Stateful
      Word Count         Processing
          |                 |
          v                 v
   Batch-wise Count    Running Count
                            |
                            v
                     Error Monitoring
```

We learned how Spark Streaming receives continuous data, divides it into micro-batches, and processes those batches using DStream transformations.

---

# 20. Final Conclusion

This project demonstrates the use of **Spark Streaming DStreams for real-time data processing**.

We implemented:

- Producer–Consumer architecture
- Netcat-based real-time data input
- Spark Streaming consumer
- DStreams
- 5-second micro-batches
- Stateless Word Count
- Stateful Word Count
- Stateful Error Counter
- Real-time log analysis
- Running counts using `updateStateByKey`

The main difference demonstrated by the project is:

```text
STATELESS
Each batch is independent.

STATEFUL
Previous results are remembered and updated.
```

Therefore, the project provides a practical demonstration of real-time stream processing using Scala and Apache Spark.
