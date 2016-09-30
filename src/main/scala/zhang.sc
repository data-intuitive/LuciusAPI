package luciusapi

import LuciusBack.AnnotatedRanks
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.util.Try
import spark.jobserver.{SparkJob, SparkJobValidation, SparkJobValid, SparkJobInvalid, NamedRddSupport}

import LuciusBack.L1000._
import LuciusBack.RddFunctions._
import LuciusBack.GeneFunctions._

object zhang extends SparkJob with NamedRddSupport {

  val _namedRdds = initialize.namedRdds

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = SparkJobValid

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
      _namedRdds.get[(String, String)](namespace + "translationTable").get.collect.toMap
    } else {
      sc.getPersistentRDDs.map { case (index, rdd) => rdd }.filter(rdd => (rdd.name == "translationTable")).head.asInstanceOf[RDD[(String,String)]].collect.toMap
    }

    // Parse the POST Request:
    // Error handling not yet ok!!
    val queryString:String = Try(config.getString("query")).getOrElse("")
    // Sort the similarities?
    val sorted:Boolean = Try(config.getString("sorted").toBoolean).getOrElse(true)

    // signature
    val untranslatedOrderedSignature = queryString.split(" ").toArray
    val translatedOrderedSignature = translateGenes(untranslatedOrderedSignature, translationTable)
    val query = orderedSignature2RankVector(translatedOrderedSignature, genes)

    // calculate zhang scores and add platewellid and index after sorting
    val annotatedSimilarity = aRanks.map(x => (connectionScore(x.ranks, query),x))
      .sortBy{case (z, x) => -z}
      .zipWithIndex
      .map{case ((z, x),index) => (index, z, x)}

    annotatedSimilarity
      .map{case (index, z, x) => (index, z, x.idx, x.id)}
      .collect()

  }

}