package part2dataframes

import org.apache.spark.sql.SparkSession


object DataFramesBasicsExercise extends App {
	/**
	  * Exercise:
	  * 1) Create a manual DF describing smartphones
	  *   - make
	  *   - model
	  *   - screen dimension
	  *   - camera megapixels
	  *
	  * 2) Read another file from the data/ folder, e.g. movies.json
	  *   - print its schema
	  *   - count the number of rows, call count()
	  */
	val spark = SparkSession.builder()
		.appName("DataFrames Basics")
		.config("spark.master", "local")
		.getOrCreate()
	import spark.implicits._

	// 1
	val smartphones = Seq(
		("Samsung", "Galaxy S10", "Android", 12),
		("Apple", "iPhone X", "iOS", 13),
		("Nokia", "3310", "THE BEST", 0)
	)

	val smartphonesDF = smartphones.toDF("Make", "Model", "Platform", "CameraMegapixels")
	smartphonesDF.show()

	// 2
	val moviesDF = spark.read
		.format("json")
		.option("inferSchema", "true")
		.load("src/main/resources/data/movies.json")
	moviesDF.printSchema()
	println(s"The Movies DF has ${moviesDF.count()} rows")

}
