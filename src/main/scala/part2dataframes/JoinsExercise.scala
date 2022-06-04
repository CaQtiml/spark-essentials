package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


/**
  * Exercises
  *
  * 1. show all employees and their max salary
  * 2. show all employees who were never managers
  * 3. find the job titles of the best paid 10 employees in the company
  */

object JoinsExercise extends App {
	val spark = SparkSession.builder()
		.appName("Joins_Exercise")
		.config("spark.master", "local")
		.getOrCreate()

	val driver = "org.postgresql.Driver"
	val url = "jdbc:postgresql://localhost:5432/rtjvm"
	val user = "docker"
	val password = "docker"

	def readTable(tableName: String) = spark.read
		.format("jdbc")
		.option("driver", driver)
		.option("url", url)
		.option("user", user)
		.option("password", password)
		.option("dbtable", s"public.$tableName")
		.load()

	val employeesDF = readTable("employees")
	val salariesDF = readTable("salaries")
	val deptManagersDF = readTable("dept_manager")
	val titlesDF = readTable("titles")

	// 1
	val salariesAgg = salariesDF
		.groupBy("emp_no")
		.agg(max("salary").as("max_salary"))
	val employeeSalaries = employeesDF.join(salariesAgg, "emp_no").drop(employeesDF.col("emp_no"))
	employeeSalaries.select("first_name","last_name", "max_salary").show()

	// 2
	val titleFilter = titlesDF.filter(col("title") =!= "Manager")
	val employeeTitles = titleFilter.join(employeesDF, "emp_no")
	employeeTitles.show()
}
