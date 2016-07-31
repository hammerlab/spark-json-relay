package org.apache.spark.ui

import org.apache.spark.scheduler.{SparkListenerJobStart, StageInfo}
import org.apache.spark.ui.scope._

import org.json4s.JsonDSL._
import org.json4s.JsonAST.JObject

/**
 * [[OperationGraph]] is used to convert [[RDDOperationGraph]] or [[StageInfo]]
 * into JSON object, in case of latter also adds stage id and attempt id.
 * Currently `node.callsite` is not supported to keep compatibility with Spark 1.5.x
 */
object OperationGraph {
  private val graph = RDDOperationGraph

  /** Convert cluster into JSON object, currently `callsite` attribute is not exported */
  private def makeJsonCluster(cluster: RDDOperationCluster): JObject = {
    val childNodes = cluster.childNodes.map { node =>
      ("id" -> node.id) ~ ("name" -> node.name) ~ ("label" -> s"${node.name} [${node.id}]") ~
        ("cached" -> node.cached) }
    val clusters = cluster.childClusters.map(makeJsonCluster)
    ("id" -> cluster.id) ~ ("name" -> cluster.name) ~ ("childNodes" -> childNodes) ~
      ("clusters" -> clusters)
  }

  /** Convert RDDOperationGraph into JSON, this includes root cluster and edges */
  def makeJson(dag: RDDOperationGraph): JObject = {
    // make subclusters
    val clustersJson = ("rootCluster" -> makeJsonCluster(dag.rootCluster))
    // make edges
    val edgesJson = ("edges" -> dag.edges.map { edge =>
      ("fromId" -> edge.fromId) ~ ("toId" -> edge.toId)
    })
    edgesJson ~ clustersJson
  }

  /** Convert StageInfo into JSON, this includes stage id and attempt id */
  def makeJsonStageDAG(stageInfo: StageInfo): JObject = {
    val dag = OperationGraph.graph.makeOperationGraph(stageInfo)
    val dagJson = makeJson(dag)
    ("stageId" -> stageInfo.stageId) ~ ("attemptId" -> stageInfo.attemptId) ~ dagJson
  }
}
