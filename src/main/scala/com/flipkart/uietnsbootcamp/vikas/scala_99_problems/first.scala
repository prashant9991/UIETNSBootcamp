package com.flipkart.uietnsbootcamp.vikas.scala_99_problems

/**
  * Created by vikas.garg on 27/12/16.
  */
object first extends App{
  println(myLast(List(1,4,2,3,1,7,2)))
  def myLast[A](l:List[A]) = l.last
}
