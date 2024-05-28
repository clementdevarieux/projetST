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

    /*
    val lines = ssc.socketTextStream("localhost", 9999)

    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCount = pairs.reduceByKey(_ + _)

    wordCount.print()

     */
    // on va chercher un fichier csv qui rentre
    // on compte le nombre de lignes du fichier
    // on print le nombre de lignes

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

    val query = read_csv.groupBy(col("Date")).agg(count(col("Date")))
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }
}


// on entraine un modele sur python, on utilise le modele ici sur le consumeur
// lui il va utiliser le modèle
// avec un point jar ?