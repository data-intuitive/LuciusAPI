package luciusapi

import LuciusBack.AnnotatedRanks
import LuciusBack.GeneFunctions._
import LuciusBack.L1000._
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import spark.jobserver._

import scala.util.Try

object compounds extends SparkJob with NamedRddSupport {

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


    val compoundQuery:String = Try(config.getString("query")).getOrElse("*")
    val compoundSpecified = !(compoundQuery == "*")

    // Return an array of entries:
    //      jnjs, pwid1, pwid2, ...
    // where pwid1 etc. are the platewellIds corresponding to the compound jnjs
    val result =
      aRanks.
        groupBy(_.jnjs).
        filter{case(jnjs,pwids) => jnjs.contains(compoundQuery) || !compoundSpecified}.
        map{case(jnjs,pwids) => (jnjs, pwids.map(_.id))}.
        collect

    result

  }

}