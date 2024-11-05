package fr.umontpelleir.ig5

import org.apache.spark.sql.SparkSession

object SimpleApp {
  def main(args: Array[String]): Unit = {
    val logFile = "data/README.md" // Should be some file on your system
    val spark = SparkSession.builder
      .appName("Simple Application")
      .master("local[*]") // on rajoute ça pour le faire en local car il est censé le faire dans un cluster
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("Spark")).count()
    val numBs = logData.filter(line => line.contains("Scala")).count()
    println(s"Lines with word Spark: $numAs,\n Lines with word Scala: $numBs")
    spark.stop()
  }
}
