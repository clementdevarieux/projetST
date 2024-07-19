package mypackage

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._
import org.apache.spark._

import java.sql.DriverManager
import java.util.Properties

import com.typesafe.config.{ Config, ConfigFactory }

object Consumer {
  def main(args: Array[String]): Unit = {

    val config: Config = ConfigFactory.load()


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
    ))

    val connectionProperties = new Properties()
    connectionProperties.put("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")

    val jdbcUrl = config.getString("db.jdbcUrl")

    val read_csv = spark.readStream
      .option("delimiter",";")
      .option("header","true")
      .schema(schema)
      .format("csv")
      .load("data/*/*.csv")


    // todo rajouter la prédiction ici
    val group_by_day = read_csv.groupBy(col("Date"))
      .agg(avg(col("Consommation (MW)")).alias("Average Consommation (MW)"),
        count("*").alias("Number of lines per day"))
      .writeStream.foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
        batchDF.foreachPartition { partition: Iterator[org.apache.spark.sql.Row] =>
          val connection = DriverManager.getConnection(jdbcUrl, connectionProperties)
          partition.foreach { row =>
            val sql = s"""
      MERGE INTO GroupByDay AS target
      USING (VALUES (?, ?, ?))
      AS source ([Date], [Average Consommation (MW)], [Number of lines per day])
      ON target.[Date] = source.[Date]
      WHEN MATCHED THEN
        UPDATE SET target.[Average Consommation (MW)] = source.[Average Consommation (MW)], target.[Number of lines per day] = source.[Number of lines per day]
      WHEN NOT MATCHED THEN
        INSERT ([Date], [Average Consommation (MW)], [Number of lines per day])
        VALUES (source.[Date], source.[Average Consommation (MW)], source.[Number of lines per day]);
      """
            val preparedStatement = connection.prepareStatement(sql)
            connection.prepareStatement(sql)
            for (i <- 0 until row.length) {
              preparedStatement.setObject(i + 1, row.get(i))
            }
            preparedStatement.executeUpdate()
          }
          connection.close()
        }
      }.outputMode("complete").start()

    val group_by_day_region = read_csv.groupBy(col("Date"),col("Région"))
      .agg(
        count("*").alias("Number of lines per day per region"),
        sum(col("Consommation (MW)")).alias("Consommation par région (MW)"),
        avg(col("Consommation (MW)")).alias("Average Consommation (MW)"),
        sum(col("Thermique (MW)")).alias("Total Thermique (MW)"),
        sum(col("Nucléaire (MW)")).alias("Total Nucléaire (MW)"),
        sum(col("Eolien (MW)")).alias("Total Eolien (MW)"),
        sum(col("Solaire (MW)")).alias("Total Solaire (MW)"),
        sum(col("Hydraulique (MW)")).alias("Total Hydraulique (MW)"),
        sum(col("Pompage (MW)")).alias("Total Pompage (MW)"),
        sum(col("Bioénergies (MW)")).alias("Total Bioénergies (MW)")
      )
      .writeStream.foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
        batchDF.foreachPartition { partition: Iterator[org.apache.spark.sql.Row] =>
          val connection = DriverManager.getConnection(jdbcUrl, connectionProperties)
          partition.foreach { row =>
            val sql = s"""
            MERGE INTO GroupByDayRegion AS target
            USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?))
            AS source ([Date], [Région], [Number of lines per day per region], [Consommation par région (MW)], [Average Consommation (MW)], [Total Thermique (MW)], [Total Nucléaire (MW)], [Total Eolien (MW)], [Total Solaire (MW)], [Total Hydraulique (MW)], [Total Pompage (MW)], [Total Bioénergies (MW)])
            ON target.[Date] = source.[Date] AND target.[Region] = source.[Région]
            WHEN MATCHED THEN
              UPDATE SET target.[Number of lines per day per region] = source.[Number of lines per day per region], target.[Consommation par région (MW)] = source.[Consommation par région (MW)], target.[Average Consommation (MW)] = source.[Average Consommation (MW)], target.[Total Thermique (MW)] = source.[Total Thermique (MW)], target.[Total Nucléaire (MW)] = source.[Total Nucléaire (MW)], target.[Total Eolien (MW)] = source.[Total Eolien (MW)], target.[Total Solaire (MW)] = source.[Total Solaire (MW)], target.[Total Hydraulique (MW)] = source.[Total Hydraulique (MW)], target.[Total Pompage (MW)] = source.[Total Pompage (MW)], target.[Total Bioénergies (MW)] = source.[Total Bioénergies (MW)]
            WHEN NOT MATCHED THEN
              INSERT ([Date], [Region], [Number of lines per day per region], [Consommation par région (MW)], [Average Consommation (MW)], [Total Thermique (MW)], [Total Nucléaire (MW)], [Total Eolien (MW)], [Total Solaire (MW)], [Total Hydraulique (MW)], [Total Pompage (MW)], [Total Bioénergies (MW)])
              VALUES (source.[Date], source.[Région], source.[Number of lines per day per region], source.[Consommation par région (MW)], source.[Average Consommation (MW)], source.[Total Thermique (MW)], source.[Total Nucléaire (MW)], source.[Total Eolien (MW)], source.[Total Solaire (MW)], source.[Total Hydraulique (MW)], source.[Total Pompage (MW)], source.[Total Bioénergies (MW)]);
            """
            val preparedStatement = connection.prepareStatement(sql)
            connection.prepareStatement(sql)
            for (i <- 0 until row.length) {
              preparedStatement.setObject(i + 1, row.get(i))
            }
            preparedStatement.executeUpdate()
          }
          connection.close()
        }
      }.outputMode("complete").start()


    val total_consommation = read_csv.agg(
        sum(col("Consommation (MW)")).alias("Consommation Totale (MW)"),
        sum(col("Thermique (MW)")).alias("Total Thermique (MW)"),
        sum(col("Nucléaire (MW)")).alias("Total Nucléaire (MW)"),
        sum(col("Eolien (MW)")).alias("Total Eolien (MW)"),
        sum(col("Solaire (MW)")).alias("Total Solaire (MW)"),
        sum(col("Hydraulique (MW)")).alias("Total Hydraulique (MW)"),
        sum(col("Pompage (MW)")).alias("Total Pompage (MW)"),
        sum(col("Bioénergies (MW)")).alias("Total Bioénergies (MW)"))
      .writeStream.foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
        batchDF.foreachPartition { partition: Iterator[org.apache.spark.sql.Row] =>
          val connection = DriverManager.getConnection(jdbcUrl, connectionProperties)
          try {
            val deleteSQL = "DELETE FROM TotalConsommation"
            val deleteStmt = connection.prepareStatement(deleteSQL)
            deleteStmt.executeUpdate()
            deleteStmt.close()

            partition.foreach { row =>
              val insertSQL =
                """
                INSERT INTO TotalConsommation ([Consommation Totale (MW)], [Total Thermique (MW)], [Total Nucléaire (MW)], [Total Eolien (MW)], [Total Solaire (MW)], [Total Hydraulique (MW)], [Total Pompage (MW)], [Total Bioénergies (MW)])
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """
              val preparedStatement = connection.prepareStatement(insertSQL)
              for (i <- 0 until row.length) {
                preparedStatement.setObject(i + 1, row.get(i))
              }
              preparedStatement.executeUpdate()
              preparedStatement.close()
            }
          } finally {
            connection.close()
          }
        }
      }.outputMode("complete").start()

    val group_by_region = read_csv.groupBy(col("Région"))
      .agg(
        sum(col("Consommation (MW)")).alias("Consommation par région (MW)"),
        sum(col("Thermique (MW)")).alias("Total Thermique (MW)"),
        sum(col("Nucléaire (MW)")).alias("Total Nucléaire (MW)"),
        sum(col("Eolien (MW)")).alias("Total Eolien (MW)"),
        sum(col("Solaire (MW)")).alias("Total Solaire (MW)"),
        sum(col("Hydraulique (MW)")).alias("Total Hydraulique (MW)"),
        sum(col("Pompage (MW)")).alias("Total Pompage (MW)"),
        sum(col("Bioénergies (MW)")).alias("Total Bioénergies (MW)"))
      .writeStream.foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
        batchDF.foreachPartition { partition: Iterator[org.apache.spark.sql.Row] =>
          val connection = DriverManager.getConnection(jdbcUrl, connectionProperties)
          partition.foreach { row =>
            val sql = s"""
            MERGE INTO GroupByRegion AS target
            USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?))
            AS source ([Région], [Consommation par région (MW)], [Total Thermique (MW)], [Total Nucléaire (MW)], [Total Eolien (MW)], [Total Solaire (MW)], [Total Hydraulique (MW)], [Total Pompage (MW)], [Total Bioénergies (MW)])
            ON target.[Region] = source.[Région]
            WHEN MATCHED THEN
            UPDATE SET
                [Consommation par région (MW)] = source.[Consommation par région (MW)],
                [Total Thermique (MW)] = source.[Total Thermique (MW)],
                [Total Nucléaire (MW)] = source.[Total Nucléaire (MW)],
                [Total Eolien (MW)] = source.[Total Eolien (MW)],
                [Total Solaire (MW)] = source.[Total Solaire (MW)],
                [Total Hydraulique (MW)] = source.[Total Hydraulique (MW)],
                [Total Pompage (MW)] = source.[Total Pompage (MW)],
                [Total Bioénergies (MW)] = source.[Total Bioénergies (MW)]
            WHEN NOT MATCHED THEN
              INSERT ([Region], [Consommation par région (MW)], [Total Thermique (MW)], [Total Nucléaire (MW)], [Total Eolien (MW)], [Total Solaire (MW)], [Total Hydraulique (MW)], [Total Pompage (MW)], [Total Bioénergies (MW)])
              VALUES (source.[Région], source.[Consommation par région (MW)], source.[Total Thermique (MW)], source.[Total Nucléaire (MW)], source.[Total Eolien (MW)], source.[Total Solaire (MW)], source.[Total Hydraulique (MW)], source.[Total Pompage (MW)], source.[Total Bioénergies (MW)]);
            """
            val preparedStatement = connection.prepareStatement(sql)
            connection.prepareStatement(sql)
            for (i <- 0 until row.length) {
              preparedStatement.setObject(i + 1, row.get(i))
            }
            preparedStatement.executeUpdate()
          }
          connection.close()
        }
      }.outputMode("complete").start()

    total_consommation.awaitTermination()
    group_by_day.awaitTermination()
    group_by_day_region.awaitTermination()
    group_by_region.awaitTermination()
  }
}


// on entraine un modele sur python, on utilise le modele ici sur le consumeur
// lui il va utiliser le modèle
// avec un point jar ?