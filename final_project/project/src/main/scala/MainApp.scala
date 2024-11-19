package fr.umontpelleir.ig5

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, explode, when}
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.io.File
import java.nio.charset.StandardCharsets

object MainApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("CVE Extractor Application")
      .master("local[*]")
      .config("spark.driver.memory", "4g")
      .config("spark.executor.memory", "4g")
      .getOrCreate()

    val inputDir = "src/main/resources/CVE_files"
    val outputFile = "src/main/resources/output/processed_cve_data_local.json"

    val jsonFiles = getListOfFiles(inputDir)
    val dataFrames = jsonFiles.map(processJsonFile(spark, _))
    val combinedData: DataFrame = dataFrames.reduce(_ union _)

    // Collecte des données en JSON pour les écrire dans un fichier
    val jsonData = combinedData.toJSON.collect().mkString("[", ",", "]")
    writeToFile(outputFile, jsonData)

    println(s"Données extraites et sauvegardées dans $outputFile")
    spark.stop()
  }

  private def getListOfFiles(dir: String, ext: String = "json"): Seq[String] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toSeq
        .filter(_.getName.endsWith(s".$ext"))
        .map(_.getPath)
    } else {
      Seq.empty[String]
    }
  }

  private def processJsonFile(spark: SparkSession, filePath: String): DataFrame = {
    val jsonData = spark.read.option("multiline", "true").json(filePath)
    jsonData
      .select(explode(col("CVE_Items")).as("item"))
      .select(
        col("item.cve.CVE_data_meta.ID").as("ID"),
        col("item.cve.description.description_data")(0)("value").as("Description"),
        when(col("item.impact.baseMetricV3").isNotNull, col("item.impact.baseMetricV3.cvssV3.baseScore"))
          .as("baseScore"),
        when(col("item.impact.baseMetricV3").isNotNull, col("item.impact.baseMetricV3.cvssV3.baseSeverity"))
          .as("baseSeverity"),
        when(col("item.impact.baseMetricV3").isNotNull, col("item.impact.baseMetricV3.exploitabilityScore"))
          .as("exploitabilityScore"),
        when(col("item.impact.baseMetricV3").isNotNull, col("item.impact.baseMetricV3.impactScore"))
          .as("impactScore")
      )
  }

  private def writeToFile(path: String, data: String): Unit = {
    val outputPath = Paths.get(path)
    Files.createDirectories(outputPath.getParent)

    // Supprimer le fichier s'il existe pour éviter les erreurs de permission
    if (Files.exists(outputPath)) {
      Files.delete(outputPath)
    }

    // Écriture des données dans le fichier
    Files.write(outputPath, data.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE_NEW)
  }
}
