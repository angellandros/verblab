package ir.angellandros.verblab

import java.io._
import scala.io._

object VerbDetector {
  val delimiters = List(".", ",", "!", "?", ":", ";", "،", "؟", "؛",
      "0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
      "۰", "۱", "۲", "۳", "۴", "۵", "۶", "۷", "۸", "۹",
      "(", ")", "[", "]", "{", "}", "<", ">", "«", "»")
  
  def reduceByKey[K,V](collection: Traversable[Tuple2[K, V]])(implicit num: Numeric[V]) = {    
    import num._
    collection
      .groupBy(_._1)
      .map { case (group: K, traversable) => traversable.reduce{(a,b) => (a._1, a._2 + b._2)} }
      .toList
  }
  
  def reduceByKeyPlus[K,V](collection: Traversable[Tuple2[K, V]])(f: (V,V) => V)(implicit num: Numeric[V]) = {    
    import num._
    collection
      .groupBy(_._1)
      .map { case (group: K, traversable) => traversable.reduce{(a,b) => (a._1, f(a._2, b._2))} }
      .toList
  }
  
  // this code is broken
  def myReduceByKey[K,V](list: List[(K,V)])(f: (V,V) => V) = {
    def groupedReduce(list: List[(K,V)]): List[(K,V)] = {
      if(list.isEmpty) List()
      else {
        val t = list.head._1
        list.filter(_._1 == t).reduce((A,B) => (A._1, f(A._2, B._2))) :: groupedReduce(list.filter(_._1 != t))
      }
    }
    groupedReduce(list)
  }
  
  def nullize[K](vec: List[(K, Int)]) = vec.map(p => (p._1, 0))
  
  def addNulls[K](vec1: List[(K, Int)], vec2: List[(K, Int)]) = reduceByKey(vec1 ::: nullize(vec2))
  
  def predot[K](vec1: List[(K, Int)], vec2: List[(K, Int)]) =
    reduceByKeyPlus(addNulls(vec1, vec2) ::: addNulls(vec2, vec1))(_*_)
  
  def dot[K](vec1: List[(K, Int)], vec2: List[(K, Int)]) =
    predot(vec1, vec2).map(p => p._2).reduce(_+_)
  
  def tokenize(s: String, d: List[String]): String =
    if(d isEmpty) s
    else tokenize(s.replace(d.head, ""), d.tail)
    
  def nSplit(s: String) = tokenize(s, delimiters).replace("ی", "ي").split(" ")
  
  def load(path: String) = {
    val folder: java.io.File = new File(path)
    val files = folder.listFiles().map(_.getPath())
    var list: List[String] = List()
    for (p <- files)
      list ++= List(scala.io.Source.fromFile(p).mkString)
    list
  }
  
  def loadWithDepth(path: String, a: Int, b: Int) = {
    var list: List[String] = List()
    for(i <- a to b) {
      list ++= load(path+i)
      print(i + " ")
    }
    list
  }
  
  def stemize1(s: String) = {
    val l = s.length
    if(s.slice(l-2, l) == "يم") s.slice(0,l-2)
    else ""
  }
  
  def stemize2(s: String) = {
    val l = s.length
    if(s.slice(l-2, l) == "يد") s.slice(0,l-2)
    else ""
  }
  
  def stemize3(s: String) = {
    val l = s.length
    if(s.slice(l-2, l) == "ند") s.slice(0,l-2)
    else ""
  }
  
  def main(args: Array[String]) {
    var path = "/home/aerabi/Dev/Datasets/Hamshahri/2007"
    if(args.length >= 1)
      path = args(0)
      
    println("loading data")
    var docs: List[String] = List()
    if(args.length == 3) docs = loadWithDepth(path, args(1).toInt, args(2).toInt)
    else docs = load(path)
    println("loaded " + docs.length + " docs successfully")
    
    var list = docs.flatMap(nSplit)
    println("splitted docs into " + list.length + " words")
    
    val counts = reduceByKey(list.map(w => (w,1))).sortBy(_._2)
    println("counted words: exists " + counts.length + " unique words")
    
    val words = counts.map(p => p._1)
    val stem1 = reduceByKey(counts.map(p => (stemize1(p._1), p._2)))
    val stem2 = reduceByKey(counts.map(p => (stemize2(p._1), p._2)))
    val stem3 = reduceByKey(counts.map(p => (stemize3(p._1), p._2)))
    println("reduced derivates")
    
    val stems = predot(predot(stem1, stem2), stem3).filter(_._2 != 0).sortBy(_._2)
    print(stems)
    
    println()
    println("number of 1st persion plurals: " + predot(stem1, stems).length)
    println("number of 2nd persion plurals: " + predot(stem2, stems).length)
    println("number of 3rd persion plurals: " + predot(stem3, stems).length)
    
    val out = new PrintWriter("verbStems.txt", "UTF-8")
    for(p <- stems) out.print(p._1 + " " + p._2 + "\n")
    out.close()
  }
}
