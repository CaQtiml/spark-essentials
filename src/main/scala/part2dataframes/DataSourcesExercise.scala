package part2dataframes

import org.apache.spark.sql.SparkSession

object DataSourcesExercise extends App {
	val spark = SparkSession.builder()
		.appName("Data Sources and Formats")
		.config("spark.master", "local")
		.getOrCreate()

	val moviesDF = spark.read.json("src/main/resources/data/movies.json")

	// TSV
	moviesDF.write
		.format("csv")
		.option("header", "true")
		.option("sep", "\t") // tab-separated CSV
		.save("src/main/resources/data/movies.csv")

	// Parquet
	moviesDF.write.save("src/main/resources/data/movies.parquet")

	val driver = "org.postgresql.Driver"
	val url = "jdbc:postgresql://localhost:5432/rtjvm"
	val user = "docker"
	val password = "docker"
	// save to DF
	moviesDF.write
		.format("jdbc")
		.option("driver", driver)
		.option("url", url)
		.option("user", user)
		.option("password", password)
		.option("dbtable", "public.movies")
		.save()
}
