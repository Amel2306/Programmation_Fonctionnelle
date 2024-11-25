package fr.umontpelleir.ig5

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
object ReadJSON {
  def main(args: Array[String]): Unit = {
    val programStartTime = System.nanoTime()
    Logger.getLogger("org").setLevel(Level.ERROR)


    val spark = SparkSession.builder()
      .appName("JSONRead")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read
      // .option("multiLine", true)
      .option("mode", "PERMISSIVE")
      .json("data/people.jsonl")

    df.printSchema()

    df.select("age", "name").show(5)


    val programElapsedTime = (System.nanoTime() - programStartTime) / 1e9
    println(s"\nProgram execution time: $programElapsedTime seconds")
    spark.stop()
  }
}
