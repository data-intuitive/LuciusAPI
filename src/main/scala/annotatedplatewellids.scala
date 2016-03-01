package luciusapi
import LuciusBack.AnnotatedRanks
import LuciusBack.GeneFunctions._
import LuciusBack.L1000._
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import spark.jobserver._

import scala.util.Try

object annotatedplatewellids extends SparkJob with NamedRddSupport {

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
    val genes = if (jobServerRunning.isDefined) {
      _namedRdds.get[String](namespace + "genes").get.collect
    } else {
      sc.getPersistentRDDs.map { case (index, rdd) => rdd }.filter(rdd => (rdd.name == "genes")).head.asInstanceOf[RDD[String]].collect
    }
    val translationTable = if (jobServerRunning.isDefined) {
      _namedRdds.get[(String, String)](namespace + "translationTable").get.collect
    } else {
      sc.getPersistentRDDs.map { case (index, rdd) => rdd }.filter(rdd => (rdd.name == "translationTable")).head.asInstanceOf[RDD[(String,String)]].collect
    }

    val pwidsString:String = Try(config.getString("pwids")).getOrElse("*")
    val featuresString:String = Try(config.getString("features")).getOrElse("*")

    // signature
    val queryString:String = Try(config.getString("query")).getOrElse("*")
    val untranslatedOrderedSignature = queryString.split(" ").toArray
    val translatedOrderedSignature = translateGenes(untranslatedOrderedSignature, translationTable.toMap)
    val query = orderedSignature2RankVector(translatedOrderedSignature, genes)

    // Are specific pwids specified?
    val pwidsSpecified = !(pwidsString == "*")
    val pwidsQuery = pwidsString.split(" ").toList

    // Are columns selected?
    val featuresList = List("id","jnjs", "jnjb", "smiles", "inchikey",
      "compoundname", "Type", "targets", "batch",
      "plateid", "well", "protocolname", "concentration", "year")
    val featuresSpecified = !(featuresString == "*")
    val featuresQuery = {
      if (featuresSpecified) featuresString.split(" ").toList
      else featuresList
    }

    // Filter on pwid selection if required
    val pwidSelection = aRanks.filter(x => (pwidsString.contains(x.id) || !pwidsSpecified ))

    // zhang scores
    val pwidSelectionZhang =  pwidSelection.map(x => (connectionScore(x.ranks, query),x))

    // Sort on similarity and index
    val sortedPwidSelection = pwidSelectionZhang
                                .sortBy{case (z, x) => -z}
                                .zipWithIndex
                                .map{case ((z, x),index) => (index, z, x)}

    // Filter features if required
    val pwidSelectionFeatures = sortedPwidSelection
      .map{case (i, score, x) => featureSelection(x, featuresQuery) ++ List(score)}

    // return result
    pwidSelectionFeatures.collect().map(_.zip(featuresQuery ++ List("zhang")).toMap.map(_.swap))

  }

}

