package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

object DataSources extends App {
	val spark = SparkSession.builder()
		.appName("Data Sources and Formats")
		.config("spark.master", "local")
		.getOrCreate()

	val carsSchema = StructType(Array(
		StructField("Name", StringType),
		StructField("Miles_per_Gallon", DoubleType),
		StructField("Cylinders", LongType),
		StructField("Displacement", DoubleType),
		StructField("Horsepower", LongType),
		StructField("Weight_in_lbs", LongType),
		StructField("Acceleration", DoubleType),
		StructField("Year", DateType),
		StructField("Origin", StringType)
	))

	/*
	  Reading a DF:
	  - format
	  - schema or inferSchema = true
	  - path
	  - zero or more options
	 */
	val carsDF = spark.read
		.format("json")
		.schema(carsSchema) // enforce a schema
		.option("mode", "failFast") // mode is a parameter: failFast, dropMalformed, permissive (default param)
		.option("path", "src/main/resources/data/cars.json")
		.load()
	/*
		.option("path", "src/main/resources/data/cars.json")
		.load()
		can be replaced by
		.load("src/main/resources/data/cars.json")
	 */

	carsDF.show();

	// alternative reading with options map
	val carsDFWithOptionMap = spark.read
		.format("json")
		.options(Map(
			"mode" -> "failFast",
			"path" -> "src/main/resources/data/cars.json",
			"inferSchema" -> "true"
		))
		.load()

	/*
	 Writing DFs
	 - format
	 - save mode = overwrite, append, ignore, errorIfExists
	 - path
	 - zero or more options
	*/
	carsDF.write
		.format("json")
		.mode(SaveMode.Overwrite)
		.save("src/main/resources/data/cars_dupe.json")

	// JSON flags
	spark.read
		.schema(carsSchema)
		.option("dateFormat", "YYYY-MM-dd") // couple with schema; if Spark fails parsing, it will put null
		.option("allowSingleQuotes", "true") // treat ' in the same way as  "
		.option("compression", "uncompressed") // bzip2, gzip, lz4, snappy, deflate
		.json("src/main/resources/data/cars.json")

	// CSV flags
	val stocksSchema = StructType(Array(
		StructField("symbol", StringType),
		StructField("date", DateType),
		StructField("price", DoubleType)
	))

	spark.read
		.schema(stocksSchema)
		.option("dateFormat", "MMM dd YYYY")
		.option("header", "true") // CSV may have or haven't the header
		.option("sep", ",")
		.option("nullValue", "") // parse "" as null
		.csv("src/main/resources/data/stocks.csv")

	// Parquet: use really often
	carsDF.write
		.mode(SaveMode.Overwrite)
		.save("src/main/resources/data/cars.parquet")

	// Text files
	spark.read.text("src/main/resources/data/sample_text.txt").show()

	// Reading from a remote DB
	val driver = "org.postgresql.Driver"
	val url = "jdbc:postgresql://localhost:5432/rtjvm"
	val user = "docker"
	val password = "docker"

	val employeesDF = spark.read
		.format("jdbc")
		.option("driver", driver)
		.option("url", url)
		.option("user", user)
		.option("password", password)
		.option("dbtable", "public.employees")
		.load()
	employeesDF.show()

}
