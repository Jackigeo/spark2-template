package sql_practice

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import spark_helpers.SessionBuilder

object examples {
  def exec1(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val toursDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")
    toursDF.show

    println(toursDF
      .select(explode($"tourTags"))
      .groupBy("col")
      .count()
      .count()
    )

    toursDF
      .select(explode($"tourTags"), $"tourDifficulty")
      .groupBy($"col", $"tourDifficulty")
      .count()
      .orderBy($"count".desc)
      .show(10)

    toursDF.select($"tourPrice")
      .filter($"tourPrice" > 500)
      .orderBy($"tourPrice".desc)
      .show(20)


  }


  def exec2(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val communeDF = spark.read
      .option("mode", "PERMISSIVE")
      .json("data/input/demographie_par_commune.json")

        println("       EX2"
    )
    println("How many inhabitants in FR?"
    )

    communeDF
      .agg(sum("Population"))
      .show


    println("Highly populated Deps : "
    )

    communeDF
      .groupBy("Departement")
      .agg(sum("Population").
        as("pop"))
      .sort($"pop".desc)
      .show()


    val depDF = spark.read
      .option("mode", "PERMISSIVE")
      .csv("data/input/departements.txt")


    println("Highly populated Deps with names : "
    )

    communeDF.
      groupBy($"Departement").
      agg(sum($"Population")
        .as("pop"))
      .sort($"pop".desc)
      .join(depDF,communeDF("Departement") === depDF("_c1"),"inner")
      .show()
  }


  def exec3(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    println("       EX3"
    )

    val s07DF = spark.read
      .option("mode", "PERMISSIVE")
      .option("delimiter", "\t")
      .csv("data/input/sample_07")
      .withColumnRenamed("_c0", "id")
      .withColumnRenamed("_c1", "description")
      .withColumnRenamed("_c2", "emp_number")
      .withColumnRenamed("_c3", "salary")


    val s08DF = spark.read
      .option("mode", "PERMISSIVE")
      .option("delimiter", "\t")
      .csv("data/input/sample_08")
      .withColumnRenamed("_c0","id")
      .withColumnRenamed("_c1","description")
      .withColumnRenamed("_c2","emp_number")
      .withColumnRenamed("_c3","salary")


    println("top 2007 salaries > 100k :"
    )

    s07DF
      .select("description","salary")
      .where(s07DF("salary")>100000)
      .sort($"salary".desc)
      .show

    println("sorted salary growth (2007-2008):"
    )

    s07DF
      .join(s08DF, s07DF("id") === s08DF("id"), "inner")
      .select(s07DF.col("description"),(s08DF.col("salary")-s07DF.col("salary")).as("salary_growth"))
      .sort($"salary_growth".desc)
      .show


    println("job loss among top earning (2007-2008):"
    )

    s07DF
      .join(s08DF, s07DF("id") === s08DF("id"), "inner")
      .where(s08DF("emp_number")<s07DF("emp_number") )
      .where(s07DF("salary")>100000)
      .select(s07DF.col("description"), (s07DF.col("emp_number") - s08DF.col("emp_number")).as("job_loss"))
      .sort($"job_loss".desc)
      .limit(10)
      .show
  }


  def exec4(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    println("       EX4"
    )


    val toursDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")

    toursDF.show

    println("Unique difficulty level count :"
    )
    println( toursDF
      .select("tourDifficulty")
      .distinct
      .count()
    )


    println("min/max/average of tour prices :"
    )

    toursDF
      .agg(max(toursDF("tourPrice")), min(toursDF("tourPrice")), avg(toursDF("tourPrice"))).show

    println("min/max/average of tour prices for each difficulty :"
    )

    toursDF
      .groupBy(toursDF("tourDifficulty"))
      .agg(max(toursDF("tourPrice")), min(toursDF("tourPrice")), avg(toursDF("tourPrice"))).show


    println("min/max/average of tour prices  and min/max/average of tour duraction for each difficulty :"
    )

    toursDF
      .groupBy(toursDF("tourDifficulty"))
      .agg(max(toursDF("tourPrice")), min(toursDF("tourPrice")), avg(toursDF("tourPrice")),max(toursDF("tourLength")), min(toursDF("tourLength")), avg(toursDF("tourLength"))).show

    println("top 10 tags:"
    )

    toursDF
      .select(explode($"tourTags").as("tag"))
      .groupBy($"tag").count().as("count")
      .sort($"count".desc)
      .limit(10)
      .show


    println("relationship tour Tag and tour Difficulty:"
    )
    toursDF
      .select(explode($"tourTags").as("tag"),$"tourDifficulty")
      .groupBy($"tourDifficulty",$"tag").count()
      .sort($"tag".asc, $"count".desc)
      .show(100)


    println("tour Tag count and top Difficulty :"
    )
    toursDF
      .select(explode($"tourTags").as("tag"), $"tourDifficulty")
      .groupBy($"tourDifficulty", $"tag").count()
      .withColumn("rank",rank().over(Window.partitionBy($"tag").orderBy($"count".desc)))
      .filter("rank==1")
      .select($"tag", $"tourDifficulty",$"count")
      .sort($"count".desc)
      .show(100)

    println(" Price average depending on tour Tag and tour Difficulty:"
    )

    toursDF
      .select(explode($"tourTags").as("tag"), $"tourDifficulty", $"tourPrice")
      .groupBy($"tourDifficulty", $"tag")
      .agg(
        min($"tourPrice").as("min_price"),
        max($"tourPrice").as("max_price"),
        avg($"tourPrice").as("avg_price"))
      .sort($"avg_price".desc)
      .show(10)


  }


}
