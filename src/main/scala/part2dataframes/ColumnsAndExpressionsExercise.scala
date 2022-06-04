package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types._

object ColumnsAndExpressionsExercise extends App {
	val spark = SparkSession.builder()
		.appName("DF Columns and Expressions Exercise")
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
	moviesDF.show(10)

	/**
	  * Exercises
	  *
	  * 1. Read the movies DF and select 2 columns of your choice
	  * 2. Create another column summing up the total profit of the movies = US_Gross + Worldwide_Gross + DVD sales
	  * 3. Select all COMEDY movies with IMDB rating above 6
	  *
	  * Use as many versions as possible
    */

	// 1
	import spark.implicits._
	val moviesReleaseDF = moviesDF.select("Title", "Release_Date")
	val moviesReleaseDF2 = moviesDF.select(
		moviesDF.col("Title"),
		col("Release_Date"),
		$"Major_Genre",
		expr("IMDB_Rating")
	)
	val moviesReleaseDF3 = moviesDF.selectExpr(
		"Title", "Release_Date"
	)

	moviesReleaseDF.show()

	// 2
	val moviesProfitDF = moviesDF.select(
		col("Title"),
		col("US_Gross"),
		col("Worldwide_Gross"),
		col("US_DVD_Sales"),
		(col("US_Gross") + col("Worldwide_Gross")).as("Total_Gross")
	)

	val moviesProfitDF2 = moviesDF.selectExpr(
		"Title",
		"US_Gross",
		"Worldwide_Gross",
		"US_Gross + Worldwide_Gross as Total_Gross"
	)

	val moviesProfitDF3 = moviesDF.select("Title", "US_Gross", "Worldwide_Gross")
		.withColumn("Total_Gross", col("US_Gross") + col("Worldwide_Gross"))

	moviesProfitDF.show()

	// 3
	val atLeastMediocreComediesDF = moviesDF.select("Title", "IMDB_Rating")
		.where(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6)

	val comediesDF2 = moviesDF.select("Title", "IMDB_Rating")
		.where(col("Major_Genre") === "Comedy")
		.where(col("IMDB_Rating") > 6)

	val comediesDF3 = moviesDF.select("Title", "IMDB_Rating")
		.where("Major_Genre = 'Comedy' and IMDB_Rating > 6")

	comediesDF3.show()
}
