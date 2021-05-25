package org.capgemini.clases.tema6.spark

import org.apache.spark.sql.SparkSession

object Joins extends App {

  val spark = SparkSession
    .builder()
    .enableHiveSupport()
    .master("local[1]")
    .appName("Spark Cap Course")
    .getOrCreate()

  import spark.implicits._


  //let's create some simple datasets
  val person = Seq((0, "Bill Chambers", 0, Seq(100)),
    (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
    (2, "Michael Armbrust", 1, Seq(250, 100)))
    .toDF("id", "name", "graduate_program", "spark_status")

  val graduateProgram = Seq(
    (0, "Masters", "School of Information", "UC Berkeley"),
    (2, "Masters", "EECS", "UC Berkeley"),
    (1, "Ph.D.", "EECS", "UC Berkeley"))
    .toDF("id", "degree", "department", "school")

  val sparkStatus = Seq(
    (500, "Vice President"),
    (250, "PMC Member"),
    (100, "Contributor"))
    .toDF("id", "status")

  person.show(false)
  graduateProgram.show(false)
  sparkStatus.show(false)

  // df.join(df2, joinCondition, joinType)

  //Inner
  val joinCondition = person.col("graduate_program") === graduateProgram.col("id")

  //person.join(graduateProgram, joinCondition, "inner").show()

  //Left - left-outer
  //graduateProgram.join(person, joinCondition, "left").show()

  //Right
  //person.join(graduateProgram, joinCondition, "right").show(false)

  //Left semi-join -> solo datos que cruzan
  //graduateProgram.join(person, joinCondition, "left_semi").show()

  //Left anti-join -> solo datos que no cruzan
  //graduateProgram.join(person, joinCondition, "left_anti").show()

  val crossJoining = person.col("graduate_program") =!= (graduateProgram.col("id") + 47)
  //Cross
  //graduateProgram.join(person, crossJoining, "inner").show()
  //graduateProgram.crossJoin(person).show()

  //Hashmap, Broadcast, Short merge, range

  //persist: graba en la memoria del disco. Ojo con guardar cosas muy grandes y que no quedarnos sin memoria

}
