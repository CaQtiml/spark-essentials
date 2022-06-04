package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, countDistinct, mean, stddev, sum}
import org.apache.spark.sql.types.{DateType, FloatType, IntegerType, LongType, StringType, StructField, StructType}


/**
  * Exercises
  *
  * 1. Sum up ALL the profits of ALL the movies in the DF
  * 2. Count how many distinct directors we have
  * 3. Show the mean and standard deviation of US gross revenue for the movies
  * 4. Compute the average IMDB rating and the average US gross revenue PER DIRECTOR
  */
object AggregationsExercise extends App {
	val spark = SparkSession.builder()
		.appName("Aggregations and Grouping")
		.config("spark.master", "local")
		.getOrCreate()

	val movieSchema = StructType(Array(
		StructField("Title", StringType),
		StructField("US_Gross", LongType),
		StructField("Worldwide_Gross", LongType),
		StructField("US_DVD_Sales", LongType),
		StructField("Production_Budget", LongType),
		StructField("Release_Date", DateType),
		StructField("MPAA_Rating", StringType),
		StructField("Running_Time_min", IntegerType),
		StructField("Distributor", StringType),
		StructField("Source", StringType),
		StructField("Major_Genre", StringType),
		StructField("Creative_Type", StringType),
		StructField("Director", StringType),
		StructField("Rotten_Tomatoes_Rating", IntegerType),
		StructField("IMDB_Rating", FloatType),
		StructField("IMDB_Votes", IntegerType)
	))

	val moviesDF = spark.read
		.schema(movieSchema) // enforce a schema
		.option("mode", "failFast") // mode is a parameter: failFast, dropMalformed, permissive (default param)
		.option("dateFormat","d-MMM-yy")
		.json("src/main/resources/data/movies.json")
	moviesDF.show()

	moviesDF
		.select((col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total_Gross"))
		.select(sum("Total_Gross"))
		.show()

	moviesDF.select(countDistinct(col("Director"))).show()

	moviesDF.agg(
		mean("US_Gross"),
		stddev("US_Gross")
	).show()

	moviesDF.groupBy("Director")
		.agg(
			mean("IMDB_Rating").as("Avg_Rating"),
			mean("US_Gross").as("Avg_US_Gross")
		)
		.orderBy(col("Avg_Rating").desc_nulls_last)
		.show()

}
