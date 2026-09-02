# Spark DataFrame Assignment

## Overview

This assignment demonstrates practical usage of **Apache Spark DataFrames, Aggregations, Window Functions, Joins, Shuffle Sort Merge Join, and Execution Plans** using Scala.

The assignment was implemented using **Scala, Apache Spark, and SBT**.

## Technologies Used

* Apache Spark
* Scala
* SBT
* Spark SQL
* Spark DataFrame API
* Linux / Ubuntu Terminal

## Project Structure

```text
spark-dataframe-assignment/
│
├── build.sbt
│
├── project/
│   └── build.properties
│
└── src/
    └── main/
        └── scala/
            └── DataFrameAssignment.scala
```

## Files

### `build.sbt`

Contains the SBT project configuration and required Spark dependencies.

### `project/build.properties`

Contains the SBT version used for the project.

### `DataFrameAssignment.scala`

Main Scala program containing all DataFrame operations, aggregations, window functions, joins, execution plan, and final exercise.

---

# 1. Sales DataFrame

A Sales DataFrame was created with the following columns:

* `customer`
* `product`
* `category`
* `price`
* `quantity`

Sample data:

| Customer | Product    | Category    | Price | Quantity |
| -------- | ---------- | ----------- | ----: | -------: |
| C001     | Laptop     | Electronics | 75000 |        2 |
| C002     | Mobile     | Electronics | 30000 |        5 |
| C001     | Chair      | Furniture   |  5000 |       10 |
| C003     | Table      | Furniture   | 10000 |        4 |
| C002     | Headphones | Electronics |  3000 |        8 |

---

# 2. Total Sales Calculation

A new column called `total` was created.

```text
total = price × quantity
```

Examples:

```text
Laptop     = 75000 × 2  = 150000
Mobile     = 30000 × 5  = 150000
Chair      = 5000 × 10  = 50000
Table      = 10000 × 4  = 40000
Headphones = 3000 × 8   = 24000
```

The `withColumn()` operation was used to create the calculated column.

---

# 3. Simple Aggregation

Simple aggregation was performed on the Sales DataFrame using:

* `sum()`
* `avg()`
* `max()`
* `min()`

Output:

```text
total_sales   = 414000.0
average_price = 24600.0
maximum_price = 75000.0
minimum_price = 3000.0
```

This provides an overall summary of the sales data.

---

# 4. Grouping Aggregation

The data was grouped by `category`.

The following values were calculated:

* Total sales
* Average sales
* Number of orders

Output:

```text
Electronics → Total Sales: 324000.0
               Average Sales: 108000.0
               Orders: 3

Furniture   → Total Sales: 90000.0
               Average Sales: 45000.0
               Orders: 2
```

The `groupBy()` operation was used along with aggregate functions.

---

# 5. Customer Aggregation

The data was grouped by customer.

The following information was calculated:

* Total spending
* Number of orders

Output:

```text
C001 → Total Spending: 200000.0
       Orders: 2

C002 → Total Spending: 174000.0
       Orders: 2

C003 → Total Spending: 40000.0
       Orders: 1
```

This shows the total amount spent by each customer.

---

# 6. Window Running Total

A Window function was used to calculate the running total of sales for each customer.

The window was partitioned by customer and ordered by product.

The operation used was:

```scala
sum("total").over(windowSpec)
```

Output example:

```text
C001 | Chair      | 50000  | Running Total = 50000
C001 | Laptop     | 150000 | Running Total = 200000

C002 | Headphones | 24000  | Running Total = 24000
C002 | Mobile     | 150000 | Running Total = 174000

C003 | Table      | 40000  | Running Total = 40000
```

This demonstrates a cumulative running total within each customer.

---

# 7. Window Ranking

The `row_number()` window function was used to rank products based on their sales within each customer.

Output:

```text
C001 | Laptop     | 150000 | Rank 1
C001 | Chair      | 50000  | Rank 2

C002 | Mobile     | 150000 | Rank 1
C002 | Headphones | 24000  | Rank 2

C003 | Table      | 40000  | Rank 1
```

The function used was:

```scala
row_number().over(rankingWindow)
```

This demonstrates Window Ranking.

---

# 8. Customer DataFrame

A separate Customer DataFrame was created with:

* `customer`
* `customer_name`
* `city`

Sample data:

```text
C001 | Harsha | Hyderabad
C002 | Ravi   | Bangalore
C003 | Priya  | Chennai
```

---

# 9. Orders DataFrame

An Orders DataFrame was created containing:

* `customer`
* `product`
* `category`
* `price`
* `quantity`
* `total`

This DataFrame was used for the join operations.

---

# 10. Simple Inner Join

The Customer DataFrame and Orders DataFrame were joined using the common `customer` column.

The join type used was:

```text
Inner Join
```

The resulting DataFrame contains customer details together with order information.

Example:

```text
C001 | Laptop | Electronics | 75000 | 2  | 150000 | Harsha | Hyderabad
C002 | Mobile | Electronics | 30000 | 5  | 150000 | Ravi   | Bangalore
C001 | Chair  | Furniture   | 5000  | 10 | 50000  | Harsha | Hyderabad
C003 | Table  | Furniture   | 10000 | 4  | 40000  | Priya  | Chennai
C002 | Headphones | Electronics | 3000 | 8 | 24000 | Ravi | Bangalore
```

---

# 11. Aggregation After Join

After joining the Customer and Orders DataFrames, aggregation was performed.

The data was grouped by:

```text
customer
customer_name
city
```

The following values were calculated:

* Total sales
* Number of orders

Output:

```text
C001 | Harsha | Hyderabad | 200000.0 | 2
C002 | Ravi   | Bangalore | 174000.0 | 2
C003 | Priya  | Chennai   | 40000.0  | 1
```

This combines customer information with their sales performance.

---

# 12. Shuffle Sort Merge Join

A **Shuffle Sort Merge Join** was demonstrated.

Broadcast join was disabled using:

```scala
spark.conf.set(
  "spark.sql.autoBroadcastJoinThreshold",
  -1
)
```

The execution plan showed:

```text
SortMergeJoin
```

The physical plan also showed:

```text
Exchange
Sort
SortMergeJoin
```

This demonstrates how Spark partitions and sorts data before performing the join.

---

# 13. Execution Plan

The Spark execution plan was displayed using:

```scala
explain(true)
```

The output contained four major sections:

```text
Parsed Logical Plan
Analyzed Logical Plan
Optimized Logical Plan
Physical Plan
```

### Parsed Logical Plan

Shows the initial logical operations requested by the program.

### Analyzed Logical Plan

Spark checks the columns, data types, and relationships between operations.

### Optimized Logical Plan

Spark optimizes the logical operations to make execution more efficient.

### Physical Plan

Shows how Spark will actually execute the operations.

The physical plan confirmed:

```text
SortMergeJoin
```

and also showed:

```text
Exchange hashpartitioning
Sort
LocalTableScan
```

---

# 14. Final Exercise

The final exercise combined:

```text
JOIN
  ↓
GROUPING
  ↓
WINDOW RANKING
```

### Step 1 – JOIN

Customer and Orders DataFrames were joined using the `customer` column.

### Step 2 – GROUPING

The joined data was grouped by customer and category.

Category-level sales were calculated.

### Step 3 – WINDOW RANKING

The `row_number()` function was applied to rank categories within each customer based on their sales.

Final output:

```text
C001 | Harsha | Hyderabad | Electronics | 150000.0 | 1
C001 | Harsha | Hyderabad | Furniture   |  50000.0 | 2

C002 | Ravi   | Bangalore | Electronics | 174000.0 | 1

C003 | Priya  | Chennai   | Furniture   |  40000.0 | 1
```

---

# 15. Main Spark Concepts Demonstrated

### DataFrames

Structured data was represented using Spark DataFrames.

### `withColumn()`

Used to create the calculated `total` column.

### `groupBy()`

Used to group records by category and customer.

### Aggregation

The following aggregate functions were used:

```text
sum()
avg()
max()
min()
count()
```

### Window Functions

Used for:

```text
Running Total
Ranking
```

### `row_number()`

Used to assign a rank to rows within each customer.

### Inner Join

Used to combine Customer and Orders DataFrames.

### SortMergeJoin

Used to demonstrate Spark's Shuffle Sort Merge Join strategy.

### `explain(true)`

Used to view Spark's complete execution plan.

---

# 16. Output Screenshots

The outputs generated during execution were captured and kept as screenshots for verification and demonstration.

The screenshots include:

1. Sales DataFrame
2. Total Sales
3. Simple Aggregation
4. Grouping Aggregation
5. Customer Aggregation
6. Window Running Total
7. Window Ranking
8. Customer DataFrame
9. Orders DataFrame
10. Simple Inner Join
11. Aggregation After Join
12. Shuffle Sort Merge Join
13. Execution Plan
14. Final Exercise

---

# 17. How to Compile

Navigate to the project directory:

```bash
cd ~/spark-assignment-2/spark-dataframe-assignment
```

Compile the project:

```bash
sbt compile
```

A successful compilation produces:

```text
[success] Total time: ...
```

---

# 18. Conclusion

This assignment provided practical experience with Spark DataFrame operations and Spark SQL concepts.

The implementation covered DataFrame creation, calculated columns, aggregation, grouping, customer-level analysis, window functions, running totals, ranking, joins, Shuffle Sort Merge Join, and execution-plan analysis.

The final exercise combined **JOIN + GROUPING + WINDOW RANKING** into one Spark workflow.
