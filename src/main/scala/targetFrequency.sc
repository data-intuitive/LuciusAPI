package luciusapi

import LuciusBack.AnnotatedRanks
import LuciusBack.L1000._
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import spark.jobserver._

import scala.util.Try


object targetFrequency extends SparkJob with NamedRddSupport {

  val _namedRdds = initialize.namedRdds

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    val namespace = sc.getConf.get("spark.app.name")

    val rdd = _namedRdds.get[String](namespace + "genes")
    if (rdd.isDefined) SparkJobValid else SparkJobInvalid(s"Missing named RDD [genes]")
  }

  override def runJob(sc: SparkContext, config: Config): Any = {

    val namespace = sc.getConf.get("spark.app.name")

    // Load cached RDDs, depending on the mode we're in
    val jobServerRunning = Try(_namedRdds).toOption
    val aRanks = if (jobServerRunning.isDefined) {
      _namedRdds.get[AnnotatedRanks](namespace + "aRanks").get
    } else {
      sc.getPersistentRDDs.map{case (index, rdd) => rdd}.filter(rdd => (rdd.name == "aRanks")).head.asInstanceOf[RDD[AnnotatedRanks]]
    }

    val pwidsString:String = Try(config.getString("pwids")).getOrElse("*")

    // Are specific pwids specified?
    val pwidsSpecified = !(pwidsString == "*")
    val pwidsQuery = pwidsString.split(" ").toList

    val targets = if (pwidsSpecified)
                    aRanks.flatMap{ aRank =>
                      if (pwidsQuery.contains(aRank.id) && aRank.targets != "")
                        aRank.targets.split(",").map(_.trim).map(target => Some((aRank.id, target)))
                      else
                        None
                    }.map(_.get).map(_.swap)
                  else
                    aRanks.flatMap{ aRank =>
                      if (aRank.targets != "")
                        aRank.targets.split(",").map(_.trim).map(target => Some((aRank.id, target)))
                      else
                        None
                    }.map(_.get).map(_.swap)


    val grouped = targets.groupByKey
    val result = grouped.map{case(gene, targetList) => (gene, targetList.size)}

    result.collect.sortBy{case(target, count) => -count}

  }

}


