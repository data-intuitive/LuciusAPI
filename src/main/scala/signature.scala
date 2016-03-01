package luciusapi

import LuciusBack.{AnnotatedRanks, AnnotatedTPStats}
import LuciusBack.GeneFunctions._
import LuciusBack.L1000._
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import spark.jobserver._
import scala.util.Try

object signature extends SparkJob with NamedRddSupport {

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
    val annotatedTPStats = if (jobServerRunning.isDefined) {
      _namedRdds.get[AnnotatedTPStats](namespace + "annotatedTPStats").get
    } else {
      sc.getPersistentRDDs.map{case (index, rdd) => rdd}.filter(rdd => (rdd.name == "annotatedTPStats")).head.asInstanceOf[RDD[AnnotatedTPStats]]
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

    val compound:String = Try(config.getString("compound")).getOrElse("*")

    // Invert the translation table, but remove identities. This way, we show the gene symbols.
    // Be carefull, the translation_table should not be a map yet, because that way some of
    // the entries are removed!
    val itt = translationTable.map(_.swap).filter{case (x,y) => x != y}.toMap

    // Start with query compound and retrieve indices
    val selection = annotatedTPStats.filter(x => x.jnjs.contains(compound)).collect
    val selectionValueVector = medianIntersection(selection)
    val selectionRankVector = valueVector2AvgRankVectorWithZeros(selectionValueVector)
    val selectionOrderedSignature = rankVector2OrderedSignature(selectionRankVector, genes)

    translateGenes(selectionOrderedSignature, itt)

  }

}
