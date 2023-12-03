//

println("==========================================================================")
println("Pipeline code . Read data form HDFS, JOINED AND performed analysis")
println("READ HDFS -> SPARK -> TRANSFORMATION - > WRITE BACK TO HDFS")
println("==========================================================================")

import org.apache.spark.sql.functions._

//load files
val sale2015DF = spark.read.format("csv").option("header", "true").load("hdfs://localhost:9000/user/input/adventureworksv2/Sales_2015.csv")
val sale2016DF = spark.read.format("csv").option("header", "true").load("hdfs://localhost:9000/user/input/adventureworksv2/Sales_2016.csv")
val sale2017DF = spark.read.format("csv").option("header", "true").load("hdfs://localhost:9000/user/input/adventureworksv2/Sales_2017.csv")

//union and add dates and year
val ordersDF = sale2015DF.union(sale2016DF).union(sale2017DF).withColumn("Order_Date", to_date(col("OrderDate"), "M/d/yyyy")).withColumn("Stock_Date", to_date(col("StockDate"), "M/d/yyyy")).withColumn("OrderYear", year(col("Order_Date")))

println("Orders joined")
println("==========================================================================")
ordersDF.show()



// load reference data
val customers = spark.read.format("csv").option("header", "true").load("hdfs://localhost:9000/user/input/adventureworksv2/Customers.csv")
val products = spark.read.format("csv").option("header", "true").load("hdfs://localhost:9000/user/input/adventureworksv2/Products.csv")



// Example : Identify the top 5 customers with the highest total sales - stich the commands together
println("Identify the top 5 customers with the highest total sales")

ordersDF.join(customers, "CustomerKey").
join(products, "ProductKey").
withColumn("qty",col("OrderQuantity").cast("int")).
withColumn("price",col("ProductPrice").cast("decimal(12,4)")).
withColumn("value",col("qty")* col("price")).
groupBy("CustomerKey", "FirstName").
agg(sum("value").as("totalsales")).
orderBy(desc("totalsales")).
limit(5).show()


// Example : Calculate the average order value per customer by children groups
println("Calculate the average order value per customer by children groups")
ordersDF.join(customers, "CustomerKey").
join(products, "ProductKey").
withColumn("cd",col("TotalChildren").cast("int")).
withColumn("childgroup", when($"cd" === 0, "No Child").when($"cd" === 1 , "one Child").when($"cd" >= 2 && $"cd" <= 3, "2-3 Child").otherwise("4+ Child")).
withColumn("qty",col("OrderQuantity").cast("int")).
withColumn("price",col("ProductPrice").cast("decimal(12,4)")).
withColumn("value",col("qty")* col("price")).
groupBy("childgroup").
agg(sum("value").as("totalsales"),avg("value").as("Average_Sales"),max("value").as("MAX_Sales"),min("value").as("Min_Sales"),count("OrderNumber").as("Count_Products"),countDistinct("OrderNumber").as("totalOrders")).
orderBy(desc("totalsales")).show()
