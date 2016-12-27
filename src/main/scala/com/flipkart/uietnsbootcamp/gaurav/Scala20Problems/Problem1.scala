package com.flipkart.uietnsbootcamp.gaurav.Scala20Problems

/**
  * Created by gaurav.malhotra on 27/12/16.
  */
object Problem1 extends App{

  println(myLast(List(1,2,3)))

  def myLast[A](list: List[A]): A={
    list.last
  }
}
