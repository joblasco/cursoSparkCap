package org.capgemini.clases.tema4.spark

import org.apache.spark.sql.functions.col

import org.capgemini.clases.tema2.spark.TestSQL.nombres

object Repartition extends App {

  val groupByBabies = nombres.groupBy(col("name")).count.foreach(_ => ())

  nombres.repartition(50).groupBy("name").count.foreach(_ => ())
  // 250 particiones: 50 del repartition porque genera shuffle y 200 del groupBy

  nombres.repartition(50, col("name")).groupBy("name").count.foreach(_ => ())
  // ahora si que va a hacer 50 particiones porque ya estamos particionando por la clave

  //internamente, hay una diferencia del algoritmo que usa según particionemos por clave o por número de particiones.
  //si es por particiones, usa un algoritmo round robin, va uno a uno mientras que cuando particionamos por clave
  //usa una función de hash (funcion que a un mismo valor, da un mismo resultado) y eso lo hace más fácil.

  //hay que tener cuidado al hacer repartition, si particionamos con pocas particiones, podemos sobresaturar la memoria
  //del executor y cargarnoslo

  //si podemos evitar hacer un repartition, mejor. Si las 200 particiones tienen datos, fenomenal. que no nos de
  //miedo tener un número alto de particiones si todas trabajan

  //que pasa si...
  nombres.repartition(50, col("year")).groupBy("name").count.foreach(_ => ())

  //coalesce

  Thread.sleep(100000000)
}
