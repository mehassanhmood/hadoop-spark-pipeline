/*Data PIPELINE

Hadoop - >CSV ->Spark -> write back to hadoop
 */


val customersDF = spark.read.format("csv").option("header", "true").load("hdfs://localhost:9000/user/input/adventureworksv2/Customers.csv")
customersDF.printSchema()
val sale2015DF = spark.read.format("csv").option("header", "true").load("hdfs://localhost:9000/user/input/adventureworksv2/Sales_2015.csv")
val joinedDF = customersDF.join(sale2015DF, "CustomerKey")
joinedDF.printSchema()
joinedDF.show()





val sale2016DF = spark.read.format("csv").option("header", "true").load("hdfs://localhost:9000/user/input/adventureworksv2/Sales_2016.csv")
val sale2017DF = spark.read.format("csv").option("header", "true").load("hdfs://localhost:9000/user/input/adventureworksv2/Sales_2017.csv")


val ordersDF1 = sale2015DF.union(sale2016DF).union(sale2017DF)


import org.apache.spark.sql.functions._

ordersDF1.show()
val ordersDF = ordersDF1.withColumn("Order_Date", to_date(col("OrderDate"), "M/d/yyyy")).withColumn("Stock_Date", to_date(col("StockDate"), "M/d/yyyy"))
ordersDF.show()


ordersDF.createOrReplaceTempView("ordersDF")
spark.sql("Select * from ordersDF").show()

//now we could have use sparksql to perform furter analysis. but lets use spark to do the same analysis


//Join with products to collect productprice and to calculate salesvalue
val productsDF = spark.read.format("csv").option("header", "true").load("hdfs://localhost:9000/user/input/adventureworksv2/Products.csv")
val joinedDFOrders = ordersDF.join(productsDF, "ProductKey")

val joinedDFOrders2=joinedDFOrders.withColumn("qty",col("OrderQuantity").cast("int")).withColumn("price",col("ProductPrice").cast("decimal(12,4)"))
val joinedDFOrders3=joinedDFOrders2.withColumn("value",col("qty")* col("price"))

 
joinedDFOrders3.show()
joinedDFOrders3.createOrReplaceTempView("joinedDFOrders3")

spark.sql("Select * from joinedDFOrders3 where qty>1").show()

// Example 2: Calculate total sales per customer
val ordersDFNew = joinedDFOrders3.groupBy("CustomerKey").agg(sum("value").as("totalSales"))
ordersDFNew.show()

// Example 3: Filter high-value customers with total sales above a threshold
val highValueCustomersDF = ordersDFNew.filter($"totalSales" > 5000)
highValueCustomersDF.show(50)

// Example 4: Calculate the average sales by ProductColor
val averageOrderValueDF = joinedDFOrders3.groupBy("ProductColor").agg(avg("value").as("avgSalesByProductColor"))
averageOrderValueDF.show(50)


// Example 5: Perform a complex transformation combining multiple operations
val transformedDF = joinedDFOrders3.join(customersDF, Seq("CustomerKey"), "left").groupBy("Occupation").agg(countDistinct("OrderNumber").as("totalOrders"), sum("value").as("totalSalesByOccupation"))
transformedDF.show()


//write back to Hadoop
 transformedDF.write.format("csv").option("header","true").save("hdfs://localhost:9000/user/input/adventureworksv2/outputn_2.csv")
val loadedDF = spark.read.format("csv").option("header", "true").load("hdfs://localhost:9000/user/input/adventureworksv2/outputn_1.csv")
loadedDF.show()






//some more complex code
