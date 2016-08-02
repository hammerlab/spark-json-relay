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

  private def edgeToJson(edge: RDDOperationEdge): JObject = {
    ("fromId" -> edge.fromId) ~ ("toId" -> edge.toId)
  }

  private def nodeToJson(node: RDDOperationNode): JObject = {
    ("rddId" -> node.id) ~ ("name" -> node.name) ~ ("cached" -> node.cached)
  }

  /** Convert StageInfo dot file into JSON, this includes stage id and attempt id */
  def makeJsonStageDAG(stageInfo: StageInfo): JObject = {
    val dag = OperationGraph.graph.makeOperationGraph(stageInfo)
    val dotFile = OperationGraph.graph.makeDotFile(dag)
    val outgoingEdges = dag.outgoingEdges.map(edgeToJson)
    val incomingEdges = dag.incomingEdges.map(edgeToJson)
    val cachedNodes = dag.rootCluster.getCachedNodes.map(nodeToJson)
    ("stageId" -> stageInfo.stageId) ~ ("attemptId" -> stageInfo.attemptId) ~
      ("dotFile" -> dotFile) ~ ("cachedRDDs" -> cachedNodes) ~
        ("incomingEdges" -> incomingEdges) ~ ("outgoingEdges" -> outgoingEdges)
  }
}
