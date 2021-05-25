package org.capgemini.clases.tema7.spark

import java.sql.Date

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.capgemini.curso.spark.utils.SparkSessionTrait

object WindowsFunctions extends SparkSessionTrait {

  def main(args: Array[String]): Unit = {
    val dataWindow: Seq[Row] = Seq(
      Row("000001", "000001.1", 100, Date.valueOf("2020-01-01")),
      Row("000001", "000001.1", 200, Date.valueOf("2020-01-02")),
      Row("000001", "000001.2", 245, Date.valueOf("2020-01-03")),
      Row("000001", "000001.2", 245, Date.valueOf("2021-01-03")),
      Row("000001", "000001.2", 245, Date.valueOf("2021-01-03")),
      Row("000001", "000001.2", 245, Date.valueOf("2021-01-03")),
      Row("000002", "000002.1", 300, Date.valueOf("2020-01-01")),
      Row("000002", "000002.1", 350, Date.valueOf("2020-01-01")),
      Row("000002", "000002.1", 300, Date.valueOf("2020-01-02")),
      Row("000002", "000002.1", 350, Date.valueOf("2020-01-05"))
    )

    val schemaWindow: StructType = StructType(
      List(
        StructField ("Cuenta", StringType, nullable = true),
        StructField ("Subcontrato", StringType, nullable = true),
        StructField ("Saldo", IntegerType, nullable = true),
        StructField ("Fecha", DateType, nullable = true)

      )
    )

    val dfGroup = spark.createDataFrame(
      spark.sparkContext.parallelize(dataWindow),
      schemaWindow
    )

    // Ejemplo max
    val window = Window.partitionBy("Cuenta")
    dfGroup.withColumn("max_saldo", max("Saldo") over window)
      .filter(col("Saldo") === col("max_saldo")).show()

    // Ejemplo fecha

    val windowFecha = Window.partitionBy("Cuenta", "Subcontrato")orderBy(desc("Fecha"))

    // Ejemplo fecha con window max fecha col
    val maxFechaDf = dfGroup.withColumn("max_fecha", row_number() over windowFecha)
      .filter(col("max_fecha") === lit("1"))

    maxFechaDf.show()

    // rank
    dfGroup.withColumn("rank_saldo", rank over windowFecha).show()

    // dense_rank
    dfGroup.withColumn("rank", dense_rank over windowFecha).show()


  }

}
