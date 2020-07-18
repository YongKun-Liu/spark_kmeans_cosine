package cluster.discosine.model

import scala.collection.immutable.Vector
import scala.io.Source
import scala.util.Random
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.mllib.util.Saveable
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import java.io.PrintWriter

import org.apache.spark.SparkContext

class Kmeans (val data:RDD[Point],numClusters:Int,MaxIterations:Int,threshold:Double=1e-4,savepath:String="/user_ext/sina_recmd_push/yongkun1/spark/Cluster") extends Serializable {

  //中心点坐标
  var CenterPoint = new Array[Point](numClusters)

  //输出该数据结构中的数据，便于调试使用
  def Output(data: RDD[Point]) {
    data.foreach { x => println(x.id + ": " + x.px.toString()) }
  }

  //获取初始的中心点
  def InitCenterRandom(random: java.util.Random) {
    val st = System.nanoTime()
    val random_seed = random.nextLong()
    CenterPoint = data.takeSample(false, numClusters, random_seed) //随机抽样
    val ed = System.nanoTime()
    println("随机中心点生成时间为：" + (ed - st))
  }

  //找到一个点距离最近的中心
  def FastSearch2(point: Iterator[Point]): Iterator[(Int,Point)] = {
    var ms:List[(Int,Point)]=List()
    while (point.hasNext) {
      val temp_point = point.next
      var cost = Double.MaxValue
      var k = -1
      for (i <- 0 until CenterPoint.length) {
        val m = temp_point.cos_distance(CenterPoint(i))
        if (cost > m) {
          cost = m
          k = i
        }
      }
      val m = Vedist(k, cost)
      ms::=(k,temp_point)
    }
    ms.iterator
  }
  def findClosest(centers:Array[Point],point: Point): (Int,Double)= {
    var bestDistance = Double.PositiveInfinity //先定义一个double型极大数
    var bestIndex = 0
    var i = 0
    centers.foreach { center =>
      // cosine distance computation.
      val distance = point.cos_distance(center)
      if (distance < bestDistance)
      {
        bestDistance=distance
        bestIndex=i
         }
        i += 1
      }
    (bestIndex, bestDistance)
  }

  //kmeans函数运行主体
  def run() {

    var converged = false
    var iteration = 0
    var cost =0.0
    val st = System.nanoTime()
    val seed = 10000l
    val random = new java.util.Random()
    random.setSeed(seed)
    InitCenterRandom(random)
    val sc = data.sparkContext  //得到spark的接口函数

    while (iteration < MaxIterations && !converged) {
      val st1 = System.currentTimeMillis()
      iteration += 1
      //计算每个点属于哪个中心点所在的类，并且记录每个类中点的数量，与该类中所有向量的和
      val costAccum = sc.doubleAccumulator //创建一个累加器 名字叫costAccum
      val bcCenters = sc.broadcast(CenterPoint)//广播centers变量 名字叫bcCenters
      val totalContribs = data.mapPartitions { points =>
        val thisCenters = bcCenters.value
        val dims = thisCenters.head.px.length
        //val sums = new Array[Point](numClusters)    //填充数组 填充中心点个数个dims维的零向量
        val sums = new ArrayBuffer[Point]()
        for (i <-0 until numClusters){
          val point = new Point(i.toString,new Array[Double](dims))
          sums.append(point)
        }
        sums.toArray
        val counts = Array.fill(thisCenters.length)(0L) //用0填充中心点个数长的数组  (0,0,0,0,0)
        points.foreach{ point =>  //foreach 和map类似 不过没有返回值
          val (bestCenter, cost) = findClosest(thisCenters,point)
          costAccum.add(cost)   // 累加器  距离总和
          val sum_center = sums(bestCenter) //点到最近距离的簇ID  sum指向sums中第几个向量
          sum_center.sum(point)
          counts(bestCenter)+=1    //第几个簇 置1  相当于（0，0，0，1，0，0）
        }
        counts.indices.filter(counts(_) > 0).map(j => (j, (sums(j), counts(j)))).iterator   //indices 返回一个range 从0到counts.length Range（0,1,2,3，5...） map之后（0，（（0,0,0），（0,0,0,1,0））） 之后用转化为一个迭代器
      }

      totalContribs.reduceByKey{ case ((sum1, count1), (sum2, count2)) =>     //reduceByKey 必须对键值对操作  合并具有相同键的值
        sum1.sum(sum2)
        (sum1, count1 + count2)
      }.collectAsMap()//用于Pair RDD，最终返回Map类型 Map(A ->B ,  C->D)

      bcCenters.destroy()

      // Update the cluster centers and costs
      converged = true
      totalContribs.foreach { case (j, (sum, count)) =>
        sum.devide(count.toInt)                   //  scal x=a*x    sum =1/count * sum

        if (converged ) {  //如果 得到的中心点减去以前的每个中心点 小于 阈值 的收敛   ///后期调整
          converged = false
        }
        CenterPoint(j) = sum
      }
      cost = costAccum.value
      iteration += 1

      val et1 = System.currentTimeMillis()
      //println("第"+k+"次聚类,sse=" + getSSE(sc,data) + ",time=" + (et1-st1)/1000+"s")
      System.gc()

      if (iteration == MaxIterations) {
        println(s"KMeans reached the max number of iterations: $MaxIterations.")
      } else {
        println(s"KMeans converged in $iteration iterations.")
      }
      println(s"The cost is $cost.")
    }
    val ed = System.nanoTime()
    println("Kmeans聚类时间为：" + (ed - st))
    //val id_val = data.map(x=>{(x.id,x.px.mkString)})
    //id_val.saveAsTextFile(savepath)
    val data_with_center = data.mapPartitions(FastSearch2)
    data_with_center.saveAsTextFile(savepath)
  }
}
