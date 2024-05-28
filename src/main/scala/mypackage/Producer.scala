package mypackage

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.count


object Producer {
  def main(args: Array[String]): Unit = {

    val logFile = "eco2mix-regional-tr.csv" // Chemin du fichier CSV

    val spark = SparkSession.builder
      .appName("Producer")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val options = Map("delimiter"->";","header"->"true")
    // Lecture du fichier CSV et création d'un DataFrame
    var logData = spark.read.options(options).csv(logFile).persist()

    // val amount_per_day = logData.groupBy(col("Date")).count()
    println("number of lines in my df " + logData.count())
    val number_of_partitions: Int = (logData.count()/50000).toInt
    println("number of partitions =" + number_of_partitions)

    // println(number_of_partitions)

    for (i <- 0 to number_of_partitions) {
      val to_write = logData.limit(50000)
      // Écrire le DataFrame actuel au format CSV
      println(s"writing to data/partition_${i}.csv")
      to_write.write
        .format("csv")
        .options(options)
        .mode("overwrite")
        .save(s"data/partition_${i}.csv")

      // Supprimer les 1152 premières lignes du DataFrame
      logData = logData.except(to_write)
      to_write.unpersist()
      println("remain lines in df " + logData.count())
      // Thread.sleep(5000)
    }
  }
}