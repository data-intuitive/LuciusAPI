package luciusapi

import LuciusBack.AnnotatedRanks
import LuciusBack.GeneFunctions._
import LuciusBack.L1000._
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import spark.jobserver._

import scala.util.Try

object platewellids extends SparkJob with NamedRddSupport {

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
    val featuresString:String = Try(config.getString("features")).getOrElse("*")

    // Are specific pwids specified?
    val pwidsSpecified = !(pwidsString == "*")
    val pwidsQuery = pwidsString.split(" ").toList

    // Are columns selected?
    val featuresSpecified = !(featuresString == "*")
    val featuresQuery = {
      if (featuresSpecified) featuresString.split(" ").toList
      else List("zhang","id","jnjs", "jnjb", "smiles", "inchikey",
                "compoundname", "Type", "targets", "batch",
                "plateid", "well", "protocolname", "concentration", "year")
    }

    // Filter on pwid selection if required
    val pwidSelection = aRanks.filter(x => (pwidsString.contains(x.id) || !pwidsSpecified ))

    // Filter features if required
    val pwidSelectionFeatures = pwidSelection.map{x => featureSelection(x, featuresQuery)}

    // return result
    pwidSelectionFeatures.collect()

  }

}
