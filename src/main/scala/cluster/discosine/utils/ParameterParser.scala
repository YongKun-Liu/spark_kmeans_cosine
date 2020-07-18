package cluster.discosine.utils

import scala.collection.mutable


/**
  * Created on 18/8/16.
  */
case class ParameterParser(args: Array[String]) {

    val argMap = new mutable.HashMap[String, String]()

    for (arg <- args) {
        val parameter = arg.trim.split("=", 2)
        if (parameter.length == 2) {
            val Array(argNames, argValue) = parameter
            argMap.+=((argNames, argValue))
        }
    }

    val day: String = argMap.getOrElse("day", "")
    val batch: String = argMap.getOrElse("batch","")

    var no_active_user: String = argMap.getOrElse("candidate_user", "")
    var user_dssm: String = argMap.getOrElse("user_dssm", "")
}

