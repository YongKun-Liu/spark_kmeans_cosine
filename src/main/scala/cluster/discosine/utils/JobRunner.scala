package cluster.discosine.utils

import cluster.discosine.extractor.Kmeans_test
import org.apache.spark.sql.SparkSession

/**
  * Created on 18/8/17.
  */
object JobRunner {
    /**
      * training
      *
      */
    def userVectorTrainJobRunner(sparkSession: SparkSession, param: ParameterParser): Unit = {

      Kmeans_test.train(sparkSession, param)
      sparkSession.sqlContext.clearCache()
    }

}
