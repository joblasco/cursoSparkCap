package org.capgemini.clases.tema5.spark

import org.apache.spark.sql.SparkSession

object Explain extends App{

  val spark = SparkSession
    .builder()
    .master("local[1]")
    .appName("Spark Cap Course")
    .getOrCreate()

  val ejemploRange = spark.range(1, 1000000)
  val rango5 = ejemploRange.selectExpr("id * 5 as id")

  rango5.explain()
/*
	Leemos el plan de abajo a arriba.
		-Primero: En este ejemplo Spark primero construye un rango de 1 a 1000000.
		-Segundo: Después Spark realiza una proyección (select)
*/

  val ejemploRangeRep = spark.range(1, 1000000)
  val rangoRep = ejemploRangeRep.repartition(7)

  rangoRep.explain()

  /*
  	Leemos el plan de abajo a arriba.
		-Primero: En este ejemplo Spark primero construye un rango de 1 a 1000000.
		-Segundo: En el segundo paso se muestra que se se está realizando un "Exchange" o "Suffle"
		que es una operación en la que se intercambian datos entre todos los ejecutores del clúster.
  */
}
