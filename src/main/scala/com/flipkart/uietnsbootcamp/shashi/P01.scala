import scala.annotation.tailrec

object P01{
  def main(args:Array[String]) :Unit= {
    println("hello world")
  }

  def last(list:List[Any]) = {
    list.last
  }

  def penultimate(list:List[Any]) ={
    if(list.length < 2) None;
    else {
      val len = list.length;
      val item = len - 2;
      val reqEle = list.zipWithIndex.filter(entry => entry._2 == item);
      Some(reqEle.head._1);
    }
  }

  def nth(pos:Int, list:List[Any])={
    if(pos < 0 || pos > list.length-1)  None
    else Some(list.zipWithIndex.filter(entry => entry._2 == pos).head._1)
  }

  def length(list:List[Any]) = {
    list.length;
  }

  def reverse(list:List[Any]) = {
    list.reverse
  }

  def isPalindrome(list:List[Int]) = {
    list.zip(list.reverse).filterNot(entry => entry._1 == entry._2).isEmpty
  }

  def flattenIt(list:List[Any]):List[Any] = {
    list.flatMap(entry => entry match{
      case list:List[_] => flattenIt(list)
      case ele => List(ele)
    })
  }

  def compress(list:List[Symbol]) = {
    @tailrec
    def runIt(list:List[Symbol], buffer:List[Symbol]):List[Symbol] = list match{
      case Nil => list
      case head::Nil => {
        buffer ++ List(head)
      }
      case head::tail =>
        if(head.equals(tail.head)) runIt(tail, buffer)
        else runIt(tail, buffer ++ List(head))
    }
    runIt(list, Nil)
  }

  def pack(list:List[Symbol]):List[List[Symbol]]= {
    if(list.isEmpty || list.length == 0) Nil
    else{
      @tailrec
      def packIt(list:List[Symbol], buffer:List[Symbol], solution:List[List[Symbol]]): List[List[Symbol]] = {
        list match{
          case Nil => solution
          case head::Nil => solution ++ List(buffer ++ List(head))
          case head::tail => {
            if(head.equals(tail.head)) packIt(tail, head::buffer, solution)
            else packIt(tail, Nil, solution ++ List(head::buffer))
          }
        }
      }
      packIt(list, Nil, Nil)
    }
  }

  def encode(list:List[Symbol]) = {
    if(list.isEmpty || list.length == 0) Nil
    else{
      val brokenList = pack(list);
      brokenList.map(entry => (entry.length, entry.head))
    }
  }

  def encodeModified(list:List[Symbol]) = {
    if(list.isEmpty || list.length == 0) Nil
    else{
      val brokenList = pack(list);
      brokenList.map{
        entry =>
          if(entry.length == 1) entry.head
          else (entry.length, entry.head)
      }
    }
  }

  def decode(list:List[(Int, Symbol)]) = {
    list.flatMap{
      entry =>
        val count = entry._1
        Range(0, count).map(iter => entry._2)
    }
  }


  def encodeDirect(list:List[Symbol]) = {
    @tailrec
    def encode(list:List[Symbol], bufferCount:Int,  solution:List[(Int, Symbol)]): List[(Int, Symbol)] = {
      list match {
        case Nil => solution
        case head::Nil => solution ++ List((bufferCount+1, head))
        case head::tail => {
          if(head.equals(tail.head)) encode(tail, bufferCount+1, solution)
          else encode(tail, 0, solution ++ List((bufferCount+1, head)))
        }
      }
    }
    if(list.isEmpty || list.length==0) Nil
    else{
      encode(list, 0, Nil)
    }
  }

  def duplicate(list:List[Symbol]) = {
    list.flatMap(entry => List(entry, entry))
  }

  def duplicateN(times: Int, list:List[Symbol]) = {
    val range = Range(0, times)
    list.flatMap{
      entry =>
        range.map(iter => entry)
    }
  }


  def drop(pos:Int, list:List[Symbol]) = {
    list.zipWithIndex.filterNot(entry => (entry._2+1)%pos == 0).map(entry => entry._1)
  }

  def split(count:Int, list:List[Symbol]) = {
    list.splitAt(count)
  }

  def slice(start:Int, end:Int, list:List[Symbol]) = {
    list.drop(start).take(end-start)
  }

  def rotate(count:Int, list:List[Symbol]) = {
    val twoLists = if(count > 0)list.splitAt(count) else list.splitAt(list.length+count)
    twoLists._2 ++ twoLists._1
  }

  def removeAt(pos:Int, list:List[Symbol]) = {
    if(list.isEmpty || list.length == 0 || pos > list.length) (Nil, Nil)
    else{
      val ele = list(pos)
      val splitList = list.splitAt(pos);
      val newList = splitList._1 ++ splitList._2.drop(1)
      (newList, ele)
    }
  }

}

