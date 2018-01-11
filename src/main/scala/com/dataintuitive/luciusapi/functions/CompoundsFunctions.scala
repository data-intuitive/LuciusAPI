package com.dataintuitive.luciusapi.functions

import com.dataintuitive.luciuscore.GeneModel.Genes
import com.dataintuitive.luciuscore.Model.DbRow
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import scala.collection.immutable.Map

object CompoundsFunctions extends SessionFunctions {

  case class JobData(db: Dataset[DbRow],
                     genes: Genes,
                     version: String,
                     compoundQuery: String,
                     limit: Int)

  type JobOutput = Array[Map[String, String]]

  val helpMsg =
    s"""Returns a list of compounds and corresponding samples matching a query, optionally with a limit on the number of results.
        |
      | Input:
        | - query: Depending on the pattern, a regexp match or `startsWith` is applied (mandatory)
        | - version: v1, v2 or t1 (optional, default is `v1`)
        | - limit: The result size is limited to this number (optional, default is 10)
     """.stripMargin

  def info(data: JobData) = s"Result for compound query ${data.compoundQuery}"

  def header(data: JobData) = "(jnjs, name)"

  def result(data: JobData)(implicit sparkSession: SparkSession) = {

    import sparkSession.implicits._

    val JobData(db, genes, version, compoundQuery, limit) = data

    // I could distinguish on version as well, but this makes more sense
    // This way, the same function can be reused for v1 and v2
    def isMatch(s: String, query: String): Boolean = {
      // Backward compatbility: Does query contains regexp or just first characters?
      val hasNonAlpha = compoundQuery.matches("^.*[^a-zA-Z0-9 ].*$")

      if (hasNonAlpha) s.matches(query)
      else s.startsWith(query)
    }

    val resultRDD =
      db.filter { sample =>
          (sample.compoundAnnotations.compound.jnjs.exists(isMatch(_, compoundQuery))
          || sample.compoundAnnotations.compound.name.exists(isMatch(_, compoundQuery)))
        }
        .map { sample =>
          (sample.compoundAnnotations.compound.getJnjs, sample.compoundAnnotations.compound.getName)
        }
        .rdd
        .countByValue()
        .keys
        .toArray

    val resultRDDasMap = resultRDD
      .map {
        case (jnjs, name) =>
          Map("jnjs" -> jnjs, "name" -> name)
      }

    val resultRDDv1 = resultRDD
      .map { case (jnjs, name) => (jnjs, name) }

    val limitOutput = (resultRDD.length > limit)

    // Should we limit the result set?
    limitOutput match {
      case true  => resultRDDasMap.take(limit)
      case false => resultRDDasMap //.collect
    }

  }

//   def compounds = result _

}
