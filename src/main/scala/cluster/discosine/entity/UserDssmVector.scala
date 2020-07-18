package cluster.discosine.entity

import cluster.discosine.utils.{ParameterParser, TimeUtils}
import org.apache.avro.generic.GenericData
import org.apache.log4j.Logger
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created  on 2020/07/01.
  * input the user vector
  */
object UserDssmVector extends Enumeration {

  @transient lazy val logg: Logger = Logger.getLogger(this.getClass)

  val userdssmSchema:StructType = StructType(Array(
    StructField("id",StringType),
      StructField("dssm",StringType)
  ))

  def getUserDssm(sparkSession: SparkSession, param: ParameterParser):  DataFrame=
  {
    val user_dssm = sparkSession.sparkContext.textFile(param.user_dssm,200)
    val user_dssm_rdd = user_dssm.map(line => {
      val lines=line.split("\t")
      if (lines.length >= 3){
        val id = lines(0)
        val dssm = lines(1)
        val iters = lines(2).toInt
        if ( id != null && dssm != null ){
          //val dssmArray=lines(1).split(",").map(_.toDouble)
          //val dssm = Vectors.dense(dssmArray)
            Row(id,dssm)
        }else{
          null
        }
      }
      else{
        null
      }
    }).filter(_!=null)
    val user_dssm_df = sparkSession.sqlContext.createDataFrame(user_dssm_rdd,userdssmSchema).distinct()
    user_dssm_df
  }
}
