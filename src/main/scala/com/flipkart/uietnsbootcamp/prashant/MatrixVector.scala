package com.flipkart.uietnsbootcamp.prashant
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import twitter4j.auth.Authorization
import twitter4j.Status
import twitter4j.auth.AuthorizationFactory
import twitter4j.conf.ConfigurationBuilder
import org.apache.spark.streaming.api.java.JavaStreamingContext
import scala.io._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.Function
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.api.java.JavaDStream
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream

object MatrixVector {

  def main(args: Array[String]) = {

    val sparkConf = new SparkConf().setAppName("MatrixVector").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val matrixF = "/Users/prashant.s/UIETNSBootcamp/matrix.txt"
    val matrix = sc.textFile(matrixF)


    val statuses = matrix.map(status =>{
      val values = status.split(",")
      (values(0).toLong,values(1).toLong,values(2).toDouble)
    })
    //val matPrint = statuses.collect.toList
    //println(matPrint)
    statuses.foreach(println)

    val vector = Source.fromFile("/Users/prashant.s/UIETNSBootcamp/vector.txt").getLines().toList
    val vectorValues= vector.map(ele =>{
      val vectVal = ele.split(",")
      (vectVal(0).toLong,vectVal(1).toDouble)
    }).toMap
    val brodVect = sc.broadcast(vectorValues)

    println(vector)
    val multipliedVal = statuses.map { case(i, j,value) => (i, value*brodVect.value(j))
    }


    val FinalMatrix = multipliedVal.reduceByKey(_+_).collect.toList

    println(FinalMatrix)


  }
}
