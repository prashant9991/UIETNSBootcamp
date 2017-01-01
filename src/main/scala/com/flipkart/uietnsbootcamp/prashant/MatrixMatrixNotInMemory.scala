package com.flipkart.uietnsbootcamp.prashant

import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by prashant.s on 31/12/16.
  */
object MatrixMatrixNotInMemory {

def main(args: Array[String]) = {

  val sparkConf = new SparkConf().setAppName("MatrixVector").setMaster("local")
  val sc = new SparkContext(sparkConf)
  val matrixF1 = "/Users/prashant.s/UIETNSBootcamp/matrix.txt"
  val matrixF2 = "/Users/prashant.s/UIETNSBootcamp/matrix2.txt"
  val matrix1 = sc.textFile(matrixF1)
  val matrix2 = sc.textFile(matrixF2)

  val matrices = matrix1.map(status =>{
    val values = status.split(",")
    (values(1).toLong,(values(0).toLong,values(2).toDouble))
  })

  val matrices2 = matrix2.map(status1 =>{
    val values2 = status1.split(",")
    (values2(0).toLong,(values2(1).toLong,values2(2).toDouble))
  })

  //val matPrint = statuses.collect.toList
  //println(matPrint)
  matrices.foreach(println)



  val finalAnswer = matrices.join(matrices2).map(entry=>{
    ((entry._2._1._1,entry._2._2._1), entry._2._2._2 * entry._2._1._2)
  }).reduceByKey(_+_)

  println(finalAnswer.collect.toList)

}

}
