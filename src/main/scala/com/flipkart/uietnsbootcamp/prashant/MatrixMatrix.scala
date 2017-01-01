package com.flipkart.uietnsbootcamp.prashant

import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source
/**
  * Created by prashant.s on 31/12/16.
  */
object MatrixMatrix {

  def main(args: Array[String]) = {

    val sparkConf = new SparkConf().setAppName("MatrixVector").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val matrixF1 = "/Users/prashant.s/UIETNSBootcamp/matrix.txt"
    val matrixF2 = "/Users/prashant.s/UIETNSBootcamp/matrix2.txt"
    val matrix1 = Source.fromFile(matrixF1).getLines().toList
    val matrix2 = sc.textFile(matrixF2)

    val matrices = matrix1.map(status =>{
      val values = status.split(",")
      (values(0).toLong,values(1).toLong,values(2).toDouble)
    })

    val globalMatrix = sc.broadcast(matrices)

    val matrices2 = matrix2.map(status1 =>{
      val values2 = status1.split(",")
      (values2(0).toLong,values2(1).toLong,values2(2).toDouble)
    })

    //val matPrint = statuses.collect.toList
    //println(matPrint)
    matrices.foreach(println)



    val FinalMatrix = matrices2.flatMap(first =>{
      globalMatrix.value.filter(element => first._2 ==element._1).map(second =>{
        ((first._1, second._2), first._3*second._3)
      })
    }).reduceByKey(_+_).collect()


    println(FinalMatrix.toList)


  }
}
