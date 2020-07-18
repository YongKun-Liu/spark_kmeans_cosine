package  cluster.discosine.extractor


import org.apache.log4j.Logger
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext._
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import cluster.discosine.entity.{ UserDssmVector}
import cluster.discosine.utils._
import cluster.discosine.model.{Kmeans, Point}

object  Kmeans_test{
  val userdssmSchema:StructType = StructType(Array(
    StructField("id",StringType),
    StructField("vector",VectorType)
  ))

  def train(sparkSession: SparkSession,param:ParameterParser): Unit= {

    //input user:vector
    val user_vector = UserDssmVector.getUserDssm(sparkSession, param)

    val noactive_user_dssm_rdd = user_vector.rdd.map(line => {
      val device_id = line.getAs[String]("id")
      val dssm = line.getAs[String]("dssm").split(",").map(_.toDouble)
      val point = new Point(device_id,dssm)
      point
    })

    //init the Kmeans class
    val k = new Kmeans(noactive_user_dssm_rdd,1000,20)
    k.run()
    println("the clusters center:")
    k.CenterPoint.map(x => {
      println(x.id+": "+x.px.mkString)
    })
    var i= 0
    val centors = k.CenterPoint.map(x => {
      i =i+1
      (i,x.px.toList)
    })

    val centors2 = sparkSession.sparkContext.parallelize(centors)
    centors2.saveAsTextFile("/yongkun1/spark/centers")

  }
  def main(args: Array[String]): Unit = {
    val warehouseLocation = "/yongkun/spark/warehouse"
    val scratchdir = "/yongkun/spark/hive-1"

    val sparkSession = SparkSession
    .builder()
      .appName("Kmeans")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("hive.exec.scratchdir", scratchdir)
      //.config("spark.master", "local")     //编辑器测试
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("DEBUG")
    val param = ParameterParser(args)

    JobRunner.userVectorTrainJobRunner(sparkSession,param)
    sparkSession.sparkContext.stop()
  }
}