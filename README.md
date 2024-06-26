# Record-Linkage
Multi Node Spark Cluster - run record deduplification using Splink

Spark Data Deduplication
This project shows a dataset deduplication process using spark, splink and Scala. A deduplication process is worth in cases where having duplicates affects either a distribuited system performance or a business metrics. Thus, this a basic example using a products dataset having similar descriptions strings, the pipeline looks for similar rows and flag/remove those that represent the same record.

Deduplicate Data Frame
For this example a couple of strategies are used. Both of them reduce the space of the problem by some assumptions. However, modify them for a real example is a matter of using the complete set of fields.

Locality-sensitive hashing (LSH)
This strategy creates a column concatenating the principal component fields PCA. For this test there is an assumption: the listed fields below are the PCA, this reduces complexity and gives a result in shorter time.

Thus, those fields are:

titleChunk
contentChunk
color
productType
Therefore, it uses a tokenizer (with word stopper - see code) to get the vector for the LSH algorithm. This creates hashes and buckets. Finally, using KNN we can query similar hashes for a category.

Pros:

Accurate: If a complete set of fields (representating the striing) is used, the correct value for hashes and neighbors could detect almost all the repeated values.
Faster: compared with other ML strategies as tfi, etc.
Cons :

Need a cluster with good resources.
Need a process for data cleaning.
To run an example: Go to the test com.sample.processor.products.ProcessorProductsLshTest and you will see a complete flow running.

Input Params:

category --> color = 'negro' and productType = 'tdi'.
nearNeighboursNumber --> 4
hashesNumber --> 3
Results:

alt text

Regular Spark Windows Functions Operations
This strategy uses spark windows operations over a multiHashing strategy. Steps:

Create a hash using some first level category field. This will be helpful for the partitioning of the data in nodes. For the example, these fields are:

productType
city
country
region
year
transmission Note: the "date" field helps to order and get only the most recent.
With this window, creates a second hash using extra cleaned parameters (after processing) a creates second window. The fields are:

doors
fuel
make
mileage
model
color
price Note: the "date" field helps to order and get only the most recent .
For each group applies levenshtein (string difference only in the second window) over the concatenated fields that changes the most and rank the window:

titleChunk
contentChunk
Finally the values with the same hashes and rank only change the rownum. Filtering rownum == 1 is possible to get the deduplicate Data set.

To run an example: Go to the test com.sample.processor.products.ProcessorProductsWindowsTest and you will see a complete flow running.

Input Params: levenshteinThreshold --> 6

Results:

alt text

The results is deduplicate after filtering rn == 1. This removes > 1/3 of the data in the sample dataset.

Pros:

More control in the spark partitioner and functions.
Cons :

Could have much more false positives.
BONUS: Calculating Metrics
After, we have a dedupliacted dataset we can safely calculate some metrics. Therefore, we will continue using Windows function to get some metrics for the products dataset. A subset of the fields and an example are found here:

com.sample.processor.products.ProductMetricsTest

An hardcode input hash category (see explantion above) is generated for some key fields [PCA] . Those key field represent filters done by an user in the platform. Example:

HashKey = concat(productType,city,country,region,transmission)

The input example is:

alt text

Then, some metrics are calculated here:


   "Starting with a simple join to understand data" should "Recognize Duplicated Data " in {

    val windowProductsKeyHash = Window.partitionBy(col("hashCategory"))
      .orderBy(col("hashCategory"))

    val windowProductsKeyHashMake = Window.partitionBy(col("hashCategory"),
      col("make")
    )

    val windowProductsKeyHashModel = Window.partitionBy(col("hashCategory"),
      col("year")
    )
      .orderBy(col("hashCategory"), col("make"))

    val productsDF = ss.createDataFrame(Seq(
      ("hash1", "make1", 50.0, 2002, "red", 10000, "1", Date.valueOf("2018-07-29")),
      ("hash1", "make1", 50.5, 2003, "red", 11000, "2", Date.valueOf("2018-07-28")),
      ("hash1", "make2", 50.6, 2004, "white", 12000, "3", Date.valueOf("2017-07-29")),
      ("hash2", "make1", 50.0, 2002, "red", 10000, "4", Date.valueOf("2017-07-29")),
      ("hash2", "make2", 50.0, 2002, "red", 11000, "5", Date.valueOf("2016-07-29")),
      ("hash2", "make3", 50.4, 2002, "red", 13000, "6", Date.valueOf("2018-07-29")),
      ("hash3", "make4", 50.0, 2005, "red", 9000, "7", Date.valueOf("2018-07-29")),
      ("hash3", "make4", 50.0, 2006, "blue", 10000, "8", Date.valueOf("2018-07-29")),
      ("hash3", "make4", 50.0, 2007, "yellow", 10000, "9", Date.valueOf("2018-07-29"))
    )).toDF("hashCategory", "make", "price", "year", "color", "mileage", "uniqueID", "date")

    productsDF.show(false)

    val productMetrics = productsDF
      .withColumn("isRecentPost", when(datediff(current_timestamp(), col("date")) > 10, 0).otherwise(1))
      .withColumn("avgPriceCategory", avg("price").over(windowProductsKeyHash))
      .withColumn("DiffAvgPrice", col("price") - col("avgPriceCategory"))
      .withColumn("makeCount", count("uniqueID").over(windowProductsKeyHashMake))
      .withColumn("rankMake", dense_rank().over(windowProductsKeyHash.orderBy(desc("makeCount"), desc("year"))))
      .withColumn("AvgkmModel", avg(col("mileage")).over(windowProductsKeyHashModel.orderBy(desc("rankMake"))))
      .withColumn("DiffAvgKms", col("mileage") - col("AvgkmModel"))
      .withColumn("NumberRecentPost", sum("isRecentPost").over(windowProductsKeyHash))
      .withColumn("newestRank", row_number().over(windowProductsKeyHash.orderBy("mileage")))
      .withColumn("isTopNew", when(col("newestRank") === 1, 1).otherwise(0))
      .withColumn("otherColors", collect_set("color").over(windowProductsKeyHash))

The result is:

alt text

The results show metrics for each product comparing itself with the rest in the same group (window).

Therefore, we have a dataframe useful to show some time series with avg prices and counts. This Dataframe will be stored in a real time series data base like Influx or elasticsearch for faster and easier lookups from a framework like tableau, etc. This metric will be shown to an user in real time while he is looking for products in a category. Example: Difference of price for products vs the rest in category.

Other databases could be taken into consideration: Cassandra, mongo, redis, etc. As future work this pipeline will be productionized using docker and deployed in an EMR cluster.
