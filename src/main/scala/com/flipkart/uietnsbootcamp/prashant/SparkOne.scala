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

object SparkOne {

  def main(args: Array[String]) = {

    val consumerKey = "GoWZLnlYrxJJ1aWtJjMUeBFSm"
    val consumerSecret = "NBb1sONMj7eiXlCen2yh0QfcvVh2zI76xoJDHhrjIiOOuf4Yu1"
    val accessToken = "852659462-CAXbgSTutslhhaEJMKQ4Hu5UaOBbPQkRDuRg0Fhe"
    val accessTokenSecret = "ojqIl9fIKfSkPTBV8OMovqxU2aNY1xSnyS0JyBDYrTIxk"
    val url = "https://stream.twitter.com/1.1/statuses/filter.json"

    val sparkConf = new SparkConf().setAppName("Twitter Streaming").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val documents: RDD[Seq[String]] = sc.textFile("").map(_.split(" ").toSeq)


    // Twitter Streaming
    val ssc = new StreamingContext(sc, Seconds(10))
    val conf = new ConfigurationBuilder()
    conf.setOAuthAccessToken(accessToken)
    conf.setOAuthAccessTokenSecret(accessTokenSecret)
    conf.setOAuthConsumerKey(consumerKey)
    conf.setOAuthConsumerSecret(consumerSecret)
    conf.setStreamBaseURL(url)
    conf.setSiteStreamBaseURL(url)

    val filter = Array("Twitter", "Hadoop", "Big Data")

    val auth = AuthorizationFactory.getInstance(conf.build())
    val tweets = TwitterUtils.createStream(ssc, Some(auth), filter)

    val statuses = tweets.map(status => status.getText)
    val words = statuses.flatMap(status => status.split(" "))
    val hashtags = words.filter(word => word.startsWith("#"))
    statuses.print()
    ssc.checkpoint("/Users/prashant.s/ScalaStart/checkpoint")
    val counts = hashtags.map(tag => (tag, 1))
      .reduceByKeyAndWindow(_ + _, _ - _, Seconds(60 * 5), Seconds(10))

    val sortedCounts = counts.map { case(tag, count) => (count, tag) }
      .transform(rdd => rdd.sortByKey(false))
    sortedCounts.foreachRDD(rdd =>
      println("\nTop 10 hashtags:\n" + rdd.take(10).mkString("\n")))
    ssc.start()
    ssc.awaitTermination()
  }
}