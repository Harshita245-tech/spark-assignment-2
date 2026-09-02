# Spark Joins Assignment

## Overview

This assignment demonstrates the four key join strategies in Apache Spark using Scala and Spark SQL/DataFrame APIs.

The assignment covers:

1. Broadcast Hash Join
2. Shuffle Hash Join
3. Shuffle Sort Merge Join
4. Broadcast Nested Loop Join

It also demonstrates how Spark chooses join strategies and how join performance can be optimized.

## Technologies Used

* Apache Spark 3.5.1
* Scala 2.12.18
* SBT
* Spark SQL
* Spark DataFrame API
* Linux / Ubuntu Terminal

## Project Structure

```text
spark-joins-assignment/
│
├── build.sbt
│
├── project/
│   └── build.properties
│
└── src/
    └── main/
        └── scala/
            └── SparkJoinsAssignment.scala
```

## Input Data

Two DataFrames were created.

### Employee DataFrame

Contains:

* `Name`
* `CountryCode`

Example:

```text
Alice  | US
Bob    | IN
Cathy  | UK
David  | US
```

### Country Lookup DataFrame

Contains:

* `CountryCode`
* `CountryName`

Example:

```text
US | United States
IN | India
UK | United Kingdom
```

The `CountryCode` column is used as the join key for the equality joins.

---

# 1. Broadcast Hash Join

A Broadcast Hash Join is useful when one dataset is small enough to be broadcast to all executors.

The smaller Country Lookup DataFrame was explicitly broadcast using:

```scala
broadcast(lookupDF)
```

The join was performed using:

```scala
employeeDF.join(
  broadcast(lookupDF),
  Seq("CountryCode"),
  "inner"
)
```

### Output

```text
+-----------+-----+-------------+
|CountryCode| Name|  CountryName|
+-----------+-----+-------------+
|         US|Alice|United States|
|         IN|  Bob|        India|
|         UK|Cathy|United Kingdom|
|         US|David|United States|
+-----------+-----+-------------+
```

### What it means

Spark sends the small lookup table to the executors so that the larger dataset does not need to shuffle the lookup data.

---

# 2. Shuffle Hash Join

Shuffle Hash Join partitions both datasets according to the join key.

Broadcasting was disabled using:

```scala
spark.conf.set(
  "spark.sql.autoBroadcastJoinThreshold",
  -1
)
```

A `SHUFFLE_HASH` hint was used to demonstrate the Shuffle Hash Join strategy.

```scala
employeeDF
  .hint("SHUFFLE_HASH")
  .join(
    lookupDF.hint("SHUFFLE_HASH"),
    Seq("CountryCode"),
    "inner"
  )
```

### What it means

Both datasets are shuffled according to the join key. Spark creates hash tables for the corresponding partitions and performs the join locally.

This strategy can be useful for medium-sized datasets when broadcasting is not suitable.

---

# 3. Shuffle Sort Merge Join

Shuffle Sort Merge Join is commonly used for large datasets and equality joins.

A `MERGE` hint was used:

```scala
employeeDF
  .hint("MERGE")
  .join(
    lookupDF.hint("MERGE"),
    Seq("CountryCode"),
    "inner"
  )
```

### Execution Plan

The execution plan shows operations such as:

```text
Exchange
Sort
SortMergeJoin
```

### What it means

Spark:

1. Shuffles the datasets based on the join key.
2. Sorts the data.
3. Merges the sorted datasets.

This is efficient for large datasets but sorting can be expensive.

---

# 4. Broadcast Nested Loop Join

Broadcast Nested Loop Join compares rows from two datasets and is useful for situations such as cross joins where there is no specific join condition.

A cross join was used:

```scala
employeeDF
  .hint("BROADCAST")
  .crossJoin(
    lookupDF.hint("BROADCAST")
  )
```

### Output

Every employee is combined with every country:

```text
Alice → US, United States
Alice → IN, India
Alice → UK, United Kingdom

Bob → US, United States
Bob → IN, India
Bob → UK, United Kingdom

Cathy → US, United States
Cathy → IN, India
Cathy → UK, United Kingdom

David → US, United States
David → IN, India
David → UK, United Kingdom
```

### What it means

A cross join creates combinations between every row of the first DataFrame and every row of the second DataFrame.

Because of the large number of possible combinations, this strategy is suitable only for small datasets.

---

# Join Strategy Comparison

| Join Strategy              | Main Idea                      | Suitable For                      |
| -------------------------- | ------------------------------ | --------------------------------- |
| Broadcast Hash Join        | Broadcast small dataset        | Small lookup/dimension tables     |
| Shuffle Hash Join          | Shuffle and hash both datasets | Medium-sized datasets             |
| Shuffle Sort Merge Join    | Shuffle, sort and merge        | Large datasets                    |
| Broadcast Nested Loop Join | Broadcast and compare rows     | Very small datasets / cross joins |

---

# Join Optimization Techniques

## 1. Broadcast Small Tables

Use:

```scala
broadcast()
```

for small lookup or dimension tables.

This can avoid unnecessary shuffling.

## 2. Repartition Data

Large datasets can be repartitioned using the join key:

```scala
repartition("CountryCode")
```

This can help reduce shuffle overhead when the data is repeatedly joined using the same key.

## 3. Sort and Bucket Data

Frequently used join keys can be sorted and bucketed to improve join performance.

## 4. Filter Early

Filters should be applied before the join whenever possible.

This reduces the amount of data that needs to participate in the join.

---

# Execution Plans

The program uses:

```scala
explain(true)
```

to display Spark's execution plan.

The execution plan contains:

```text
Parsed Logical Plan
Analyzed Logical Plan
Optimized Logical Plan
Physical Plan
```

The physical plan shows how Spark actually executes the join.

For Shuffle Sort Merge Join, the physical plan demonstrates:

```text
Exchange
Sort
SortMergeJoin
```

This shows the shuffle and sorting stages involved in the join.

---

# Compilation and Execution

Navigate to the assignment directory:

```bash
cd ~/spark-assignment-2/spark-joins-assignment
```

Compile the project:

```bash
sbt compile
```

Run the program:

```bash
sbt run
```

A successful compilation displays:

```text
[success] Total time: ...
```

The program then displays the input DataFrames, results of all four join strategies, execution plans, and join optimization techniques.

---

# Output Screenshots

The outputs generated during execution were captured as screenshots for verification and demonstration.

The screenshots include:

1. Employee DataFrame
2. Country Lookup DataFrame
3. Broadcast Hash Join output
4. Broadcast Hash Join execution plan
5. Shuffle Hash Join execution plan
6. Shuffle Sort Merge Join output
7. Shuffle Sort Merge Join execution plan
8. Broadcast Nested Loop Join output
9. Broadcast Nested Loop Join execution plan
10. Join optimization techniques

---

# Key Concepts Learned

Through this assignment, the following concepts were implemented and understood:

* Spark DataFrames
* Spark SQL joins
* Broadcast Hash Join
* Shuffle Hash Join
* Shuffle Sort Merge Join
* Broadcast Nested Loop Join
* Broadcast joins
* Data shuffling
* Hash-based joins
* Sorting
* Repartitioning
* Cross joins
* Execution plans
* `explain(true)`
* Join optimization

---

# Conclusion

This assignment provided practical experience with different join strategies in Apache Spark.

The implementation demonstrated how Spark can use different strategies depending on dataset size, partitioning, join conditions, and configuration.

The assignment also demonstrated techniques such as broadcasting small tables, repartitioning data, sorting and bucketing, and filtering data before joins to improve Spark join performance.
