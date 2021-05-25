package org.capgemini.clases.tema3.spark

import java.sql.Date

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, DateType, IntegerType, MapType, StringType, StructField, StructType}
import org.capgemini.curso.spark.utils.SparkSessionTrait

object Functions extends SparkSessionTrait{

  def main(args: Array[String]): Unit = {

    val schemaOne: StructType = StructType(
      List(
        StructField("Campo1", StringType, nullable = true),
        StructField("Campo2", StringType, nullable = true),
        StructField("Campo3", IntegerType, nullable = true)
      )
    )

    val dataOne: Seq[Row] = Seq(
      Row("VALOR1.1", "VALOR1.2", 1),
      Row("VALOR2.1", "VALOR2.2", 2)
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(dataOne),
      schemaOne
    )

    //Count
    println(df.count())

    //First
   println( df.first())

    //Select col
    df.select(col("Campo1")).show()

    //Expressions
    df.select(expr("Campo3 + 5 as Campo5")).show()
    df.select(col("Campo3").alias ("CampoAlias")).show()
    df.select(col("Campo3").as ("CampoAs")).show()

    df.select("*").show()

    //añadir columna
    df.withColumn("CampoNuevo", lit("1")).show()

    //Renombrar columna
    df.withColumnRenamed("Campo1", "CampoRenombrado")
    .withColumnRenamed("Campo2", "CampoRenombrado2")show()

    //Borrado columna
    df.drop("Campo1")

    //casteo columna
    df.select(col("Campo3").cast("String")).printSchema()

    //Filtrado
    df.filter(col("Campo3") === 1 ).show()
    println("Filtro Filter")
    df.filter(col("Campo3") === 1 and col("Campo2") === "VALOR1.2").show()
    println("Filtro con Where")
    df.where(col("Campo3") === 1 and col("Campo2") === "VALOR1.2").show()

    //distinct
    println("Distinct")
    df.select(col("Campo3")).distinct().show()

    //concat

    df.withColumn("ConcatColumn", concat(col("Campo1"), col("Campo2"))).show()

    //concat_ws
    df.withColumn("ConcatColumn", concat_ws(",", col("Campo1"), col("Campo2"))).show()

    //groupBy

      val schemaGroup: StructType = StructType(
        List(
          StructField("Cuenta", StringType, true),
          StructField("Subcontrato", StringType, true),
          StructField("Saldo", IntegerType, true),
          StructField("Fecha", DateType, true)
        )
      )


    val dataGroup: Seq[Row] = Seq(
      Row("000001", "000001.1", 100, Date.valueOf("2020-01-01")),
      Row("000001", "000001.1", 200, Date.valueOf("2020-01-02")),
      Row("000001", "000001.2", 245, Date.valueOf("2020-01-03")),
      Row("000002", "000002.1", 300, Date.valueOf("2020-01-01")),
      Row("000002", "000002.1", 350, Date.valueOf("2020-01-01")),
      Row("000002", "000002.1", 300, Date.valueOf("2020-01-02")),
      Row("000002", "000002.1", 350, Date.valueOf("2020-01-02"))
    )

    val dfGroup = spark.createDataFrame(
      spark.sparkContext.parallelize(dataGroup),
      schemaGroup
    )
    dfGroup.show()

    dfGroup.groupBy("Cuenta").agg(max("Saldo")).show()
    dfGroup.groupBy("Cuenta").agg(max("Fecha")).show()
    dfGroup.groupBy("Cuenta").agg(max("Fecha"), max("Saldo")).show()

    // orderBy

    dfGroup.groupBy("Cuenta", "Subcontrato")
      .agg(
        max("Fecha").as("Fecha"),
          max("Saldo").as("Saldo")
      )
      .orderBy(
        col("Cuenta").asc,
        col("Subcontrato").asc,
        col("Saldo").desc
      )
      .show()

  //when
    dfGroup.withColumn("CondColumn",
      when(
        col("Saldo") <= lit("200"),
        lit("RED")
      )
        .when(
          col("Saldo") > lit("200"),
          lit("GREEN")
        )
        .otherwise(lit("UNKNOWN"))
    ).show()

    //explode

    val schemaExplode: StructType = StructType(
      List(
        StructField("Cuenta", StringType, true),
        StructField("Subcontrato", StringType, true),
        StructField("Saldo", IntegerType, true),
        StructField("Fecha", DateType, true),
        StructField("MapTipoCuenta", MapType(StringType, StringType, true), true)
      )
    )

    val dataExplode: Seq[Row] = Seq(
      Row("000001", "000001.1", 100, Date.valueOf("2020-01-01"), Map("Depo" -> "Verde")),
      Row("000001", "000001.1", 200, Date.valueOf("2020-01-02"), Map("Depo" -> "Verde")),
      Row("000001", "000001.2", 245, Date.valueOf("2020-01-03"), Map("Depo" -> "Rojo")),
      Row("000002", "000002.1", 300, Date.valueOf("2020-01-01"), Map("Acc" -> "Naranja")),
      Row("000002", "000002.1", 350, Date.valueOf("2020-01-01"), Map("Acc" -> "Naranja")),
      Row("000002", "000002.1", 300, Date.valueOf("2020-01-02"), Map("Acc" -> "Naranja")),
      Row("000002", "000002.1", 350, Date.valueOf("2020-01-02"), Map("Acc" -> "Naranja"))
    )

    val dfExplode = spark.createDataFrame(
      spark.sparkContext.parallelize(dataExplode),
      schemaExplode
    )

    dfExplode.show()

    dfExplode.select(col("Cuenta"), col("Subcontrato"), explode (col("MapTipoCuenta"))).show()

    //DropDuplicates
    dfExplode.select(col("Cuenta"), col("Subcontrato"), explode (col("MapTipoCuenta"))).dropDuplicates().show()

  }
}

