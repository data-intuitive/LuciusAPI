package com.dataintuitive.luciusapi

import com.dataintuitive.luciuscore.GeneModel.{GeneAnnotation, Genes}
import com.dataintuitive.luciuscore.Model._
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  * The API makes use of `NamedObjects`, but those are actor-based so automated tests are made hard.
  * This trait can be mixed into the endpoint definitions, in order to keep a global cache of
  * otherwise persisted objects in Spark Jobserver.
  */
trait Globals extends Serializable {

  val gene: GeneAnnotation = new GeneAnnotation("probesetidString",
    "entrezidString",
    "ensemblidString",
    "symbolString",
    "nameString")

  var thisGenes:Broadcast[Genes] = _ //new Genes(Array(gene))

  def setGenes(g:Broadcast[Genes]) = { thisGenes = g }
  def getGenes = thisGenes

  def setDb(db:RDD[DbRow]) = db.cache.setName("db")
  def getDb(sc:SparkContext) =
    sc.getPersistentRDDs
      .map{case (index, rdd) => rdd}
      .filter(rdd => (rdd.name == "db"))
      .head.asInstanceOf[RDD[DbRow]]

  def getGlobals = Set("db", "genes")

}
