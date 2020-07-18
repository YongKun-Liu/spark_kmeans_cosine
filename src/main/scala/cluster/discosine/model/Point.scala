package cluster.discosine.model

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
import parquet.org.codehaus.jackson.map.ser.impl.PropertySerializerMap.Empty

class Point(val id:String,val px:Array[Double]) extends Serializable{

  def sum(that:Point):Point={
    val new_id =this.id
    val new_px =(this.px,that.px).zipped.map(_+_)
    new Point(new_id,new_px)
    }
  def devide(divisor:Int):Point={
    val new_px = this.px.map(_/divisor)
    new Point(this.id,new_px)
  }

  /**求两个向量的余弦,1-相似度，结果越大 差异越大，越小差异越小 */
  def cos_distance(that: Point) = {
    val cos = 1- innerProduct(this.px, that.px) / (module(this.px) * module(that.px))
    cos
  }
  /** 求两个向量的内积*/
  def innerProduct(v1: Array[Double], v2: Array[Double]) = {
    val listBuffer = ListBuffer[Double]()
    for (i <- 0 until v1.length; j <- 0 to v2.length; if i == j) {
      if (i == j) listBuffer.append(v1(i) * v2(j))
    }
    listBuffer.sum
  }
  /**取模运算**/
  def module(point: Array[Double]): Double ={
    val result = Math.sqrt(point.map( x => { x*x }).sum)
    result
  }


}

//单纯的储存信息的case类，center_id代表数据点对应的中心点，cost代表两点的花费
case class Vedist(val center_id:Int,val cost:Double)

object  Test {
  def main(args: Array[String]): Unit = {
    val id = "7e17107996049c64"
    val px = Array(1.0, 2.0, 3.0, 4.0)
    val point1 = new Point(id, px)

    val id2 = "7e17107996049c63"
    val px2 = Array(1.0, 2.0, 3.0, 4.0)
    val point2 = new Point(id, px)

    println("yongkun2")
    println(point1.id)
    println(point1.px.mkString)
    val point3 =point1.sum(point2)
    println(point3.px.mkString)

    val point4 = point1.devide(2)
    println(point4.px.mkString)

    val sums= new ArrayBuffer[Point](5)
    for (i <- 0 until 5)
    sums.append(point4)
    println(sums(0).px)
  }
}