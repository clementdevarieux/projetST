package mypackage

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._
import org.apache.spark._

object Consumer {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("StructuredNetworkWordCount")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val schema = StructType(Array(
      StructField("Code INSEE région", IntegerType, true),
      StructField("Région", StringType, true),
      StructField("Nature", StringType, true),
      StructField("Date", StringType, true),
      StructField("Heure", StringType, true),
      StructField("Date - Heure", StringType, true),
      StructField("Consommation (MW)", IntegerType, true),
      StructField("Thermique (MW)", IntegerType, true),
      StructField("Nucléaire (MW)", IntegerType, true),
      StructField("Eolien (MW)", IntegerType, true),
      StructField("Solaire (MW)", IntegerType, true),
      StructField("Hydraulique (MW)", IntegerType, true),
      StructField("Pompage (MW)", IntegerType, true),
      StructField("Bioénergies (MW)", IntegerType, true),
      StructField("Ech. physiques (MW)", IntegerType, true),
      StructField("Stockage batterie", StringType, true),
      StructField("Déstockage batterie", StringType, true),
      StructField("TCO Thermique (%)", DoubleType, true),
      StructField("TCH Thermique (%)", DoubleType, true),
      StructField("TCO Nucléaire (%)", DoubleType, true),
      StructField("TCH Nucléaire (%)", DoubleType, true),
      StructField("TCO Eolien (%)", DoubleType, true),
      StructField("TCH Eolien (%)", DoubleType, true),
      StructField("TCO Solaire (%)", DoubleType, true),
      StructField("TCH Solaire (%)", DoubleType, true),
      StructField("TCO Hydraulique (%)", DoubleType, true),
      StructField("TCH Hydraulique (%)", DoubleType, true),
      StructField("TCO Bioénergies (%)", DoubleType, true),
      StructField("TCH Bioénergies (%)", DoubleType, true),
      StructField("Column 68", StringType, true)
    ))

    val read_csv = spark.readStream
      .option("delimiter",";")
      .option("header","true")
      .schema(schema)
      .format("csv")
      .load("data/*/*.csv")

    val lines_per_date = read_csv.groupBy(col("Date")).count()
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()

    val lines_per_date_per_region = read_csv.groupBy(col("Date"),col("Région")).count()
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()

    val total_consommation = read_csv.agg(sum(col("Consommation (MW)")).alias("Consommation Totale (MW)"))
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()

    val consommation_per_region = read_csv.groupBy(col("Région")).agg(sum(col("Consommation (MW)")).alias("Consommation par région (MW)"))
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()

    val consommation_per_region_per_day = read_csv.groupBy(col("Région"),col("Date")).agg(sum(col("Consommation (MW)")).alias("Consommation par région (MW)"))
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()

    val production_per_energy = read_csv.agg(
        sum(col("Thermique (MW)")).alias("Total Thermique (MW)"),
        sum(col("Nucléaire (MW)")).alias("Total Nucléaire (MW)"),
        sum(col("Eolien (MW)")).alias("Total Eolien (MW)"),
        sum(col("Solaire (MW)")).alias("Total Solaire (MW)"),
        sum(col("Hydraulique (MW)")).alias("Total Hydraulique (MW)"),
        sum(col("Pompage (MW)")).alias("Total Pompage (MW)"),
        sum(col("Bioénergies (MW)")).alias("Total Bioénergies (MW)")
      )
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()

    val production_per_energy_per_region = read_csv.groupBy(col("Région")).agg(
        sum(col("Thermique (MW)")).alias("Total Thermique (MW)"),
        sum(col("Nucléaire (MW)")).alias("Total Nucléaire (MW)"),
        sum(col("Eolien (MW)")).alias("Total Eolien (MW)"),
        sum(col("Solaire (MW)")).alias("Total Solaire (MW)"),
        sum(col("Hydraulique (MW)")).alias("Total Hydraulique (MW)"),
        sum(col("Pompage (MW)")).alias("Total Pompage (MW)"),
        sum(col("Bioénergies (MW)")).alias("Total Bioénergies (MW)")
      )
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()

    val production_per_energy_per_region_per_day = read_csv.groupBy(col("Région"),col("Date")).agg(
        sum(col("Thermique (MW)")).alias("Total Thermique (MW)"),
        sum(col("Nucléaire (MW)")).alias("Total Nucléaire (MW)"),
        sum(col("Eolien (MW)")).alias("Total Eolien (MW)"),
        sum(col("Solaire (MW)")).alias("Total Solaire (MW)"),
        sum(col("Hydraulique (MW)")).alias("Total Hydraulique (MW)"),
        sum(col("Pompage (MW)")).alias("Total Pompage (MW)"),
        sum(col("Bioénergies (MW)")).alias("Total Bioénergies (MW)")
      )
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()

    val consommation_moyenne_per_day = read_csv.groupBy(col("Date"))
      .agg(avg(col("Consommation (MW)")).alias("Average Consommation (MW)"))
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()

    val consommation_moyenne_per_day_per_region = read_csv.groupBy(col("Date"),col("Région"))
      .agg(avg(col("Consommation (MW)")).alias("Average Consommation (MW)"))
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()



    lines_per_date.awaitTermination()
    lines_per_date_per_region.awaitTermination()
    total_consommation.awaitTermination()
    consommation_per_region.awaitTermination()
    consommation_per_region_per_day.awaitTermination()
    production_per_energy.awaitTermination()
    production_per_energy_per_region.awaitTermination()
    production_per_energy_per_region_per_day.awaitTermination()
    consommation_moyenne_per_day.awaitTermination()
    consommation_moyenne_per_day_per_region.awaitTermination()
  }
}


// on entraine un modele sur python, on utilise le modele ici sur le consumeur
// lui il va utiliser le modèle
// avec un point jar ?