package org.apache.spark

import java.net.ConnectException
import java.nio.charset.Charset
import java.util.Properties

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.rdd.RDDOperationScope
import org.apache.spark.scheduler._
import org.apache.spark.storage._

import org.json4s.jackson.{JsonMethods => Json}

class JsonRelaySuite extends UnitTestSuite {

  // Test assumes that slim is not runningon default localhost:8123
  test("fail with ConnectionException") {
    val conf = new SparkConf().set("spark.app.id", "test")
    intercept[ConnectException] {
      val relay = new JsonRelay(conf)
    }
  }

  test("verify default settings for slim options") {
    val relay = new TestJsonRelay("test")

    relay.appId should be ("test")
    relay.host should be ("localhost")
    relay.port should be (8123)
    relay.debugLogs should be (false)
  }

  test("verify settings provided as part of Spark configuration") {
    val conf = new SparkConf()
      .set("spark.app.id", "mytest")
      .set("spark.slim.host", "myhost")
      .set("spark.slim.port", "9999")
      .set("spark.slim.verbose", "true")
    val relay = new TestJsonRelay(conf)

    relay.appId should be ("mytest")
    relay.host should be ("myhost")
    relay.port should be (9999)
    relay.debugLogs should be (true)
  }

  test("verify internal metrics and constants are set") {
    val relay = new TestJsonRelay("test")

    relay.utf8 should be (Charset.forName("UTF-8"))
    // initial number of performed requests is zero
    relay.numReqs should be (0)
    relay.lastEvent should be (None)
  }

  test("on event - null event") {
    val relay = new TestJsonRelay("test")
    relay.onEvent(null)
    relay.getStreamContent should be ("")
  }

  test("on event - corrupt event") {
    val relay = new TestJsonRelay("test")
    intercept[NullPointerException] {
      relay.onEvent(SparkListenerJobStart(jobId = 123, time = 1L, stageInfos = null))
    }
  }

  test("on event - SparkListenerApplicationStart") {
    val appStart = SparkListenerApplicationStart("appName", Some("appId"), time = 1L,
      sparkUser = "sparkUser", appAttemptId = Some("appAttemptId"), driverLogs = None)
    val relay = new TestJsonRelay("test")
    relay.onEvent(appStart)

    checkJson(
      relay.getStreamContent,
      """
      {
        "Event": "SparkListenerApplicationStart",
        "App Name": "appName",
        "App ID": "appId",
        "Timestamp": 1,
        "User": "sparkUser",
        "App Attempt ID": "appAttemptId",
        "appId": "test"
      }
      """)
  }

  test("on event - SparkListenerApplicationEnd") {
    val appEnd = SparkListenerApplicationEnd(time = 1L)
    val relay = new TestJsonRelay("test")
    relay.onEvent(appEnd)

    checkJson(
      relay.getStreamContent,
      """
      {
        "Event": "SparkListenerApplicationEnd",
        "Timestamp": 1,
        "appId": "test"
      }
      """)
  }

  test("on event - SparkListenerJobStart") {
    val rddInfos = Seq(
      new RDDInfo(
        id = 18,
        name = "RDD 18",
        numPartitions = 100,
        storageLevel = StorageLevel.MEMORY_ONLY,
        parentIds = Seq(12, 15)))

    val stageInfos = Seq(
      new StageInfo(
        stageId = 3,
        attemptId = 0,
        name = "Stage 3",
        numTasks = 100,
        rddInfos = rddInfos,
        parentIds = Seq(0, 1, 2),
        details = "details"),
      new StageInfo(
        stageId = 4,
        attemptId = 0,
        name = "Stage 4",
        numTasks = 100,
        rddInfos = Seq.empty,
        parentIds = Seq(3),
        details = "details")
    )

    val jobStart = SparkListenerJobStart(
      jobId = 123,
      time = 1L,
      stageInfos = stageInfos)

    val relay = new TestJsonRelay("test")
    relay.onEvent(jobStart)

    checkJson(
      relay.getStreamContent,
      """
      {
        "Event": "SparkListenerJobStart",
        "Job ID": 123,
        "Submission Time": 1,
        "Stage Infos": [
          {
            "Stage ID": 3,
            "Stage Attempt ID": 0,
            "Stage Name": "Stage 3",
            "Number of Tasks": 100,
            "RDD Info": [
              {
                "RDD ID": 18,
                "Name": "RDD 18",
                "Parent IDs": [12, 15],
                "Storage Level": {
                  "Use Disk": false,
                  "Use Memory": true,
                  "Use ExternalBlockStore": false,
                  "Deserialized": true,
                  "Replication": 1
                },
                "Number of Partitions": 100,
                "Number of Cached Partitions": 0,
                "Memory Size": 0,
                "ExternalBlockStore Size": 0,
                "Disk Size": 0
              }
            ],
            "Parent IDs": [0, 1, 2],
            "Details": "details",
            "Accumulables":[]
          },
          {
            "Stage ID": 4,
            "Stage Attempt ID": 0,
            "Stage Name": "Stage 4",
            "Number of Tasks": 100,
            "RDD Info": [],
            "Parent IDs": [3],
            "Details": "details",
            "Accumulables":[]
          }
        ],
        "Stage IDs": [3, 4],
        "appId": "test"
      }
      """)
  }

  test("on event - SparkListenerJobEnd") {
    val jobEnd = SparkListenerJobEnd(
      jobId = 123,
      time = 1L,
      jobResult = JobSucceeded)

    val relay = new TestJsonRelay("test")
    relay.onEvent(jobEnd)

    checkJson(
      relay.getStreamContent,
      """
      {
        "Event": "SparkListenerJobEnd",
        "Job ID": 123,
        "Completion Time": 1,
        "Job Result": {
          "Result": "JobSucceeded"
        },
        "appId": "test"
      }
      """)
  }

  test("on event - SparkListenerStageSubmitted") {
    val rddInfo = new RDDInfo(
      id = 2,
      name = "ShuffledRDD",
      numPartitions = 4,
      storageLevel = StorageLevel.NONE,
      parentIds = Seq(1),
      scope = Some(new RDDOperationScope(id = "2", name = "reduceByKey")))

    val stageInfo = new StageInfo(
      stageId = 1,
      attemptId = 0,
      name = "count at <console>:26",
      numTasks = 4,
      rddInfos = Seq(rddInfo),
      parentIds = Seq(0),
      details = "details")
    stageInfo.submissionTime = Option(1437323864337L)

    val stageSubmitted = SparkListenerStageSubmitted(
      stageInfo = stageInfo, properties = new Properties())
    val relay = new TestJsonRelay("local-1437323845748")
    relay.onEvent(stageSubmitted)

    checkJson(
      relay.getStreamContent,
      """
      {
        "Event": "SparkListenerStageSubmitted",
        "Stage Info": {
          "Stage ID": 1,
          "Stage Attempt ID": 0,
          "Stage Name": "count at <console>:26",
          "Number of Tasks": 4,
          "RDD Info": [
            {
              "RDD ID": 2,
              "Name": "ShuffledRDD",
              "Scope": "{\"id\":\"2\",\"name\":\"reduceByKey\"}",
              "Parent IDs": [1],
              "Storage Level": {
                "Use Disk": false,
                "Use Memory": false,
                "Use ExternalBlockStore": false,
                "Deserialized": false,
                "Replication": 1
              },
              "Number of Partitions": 4,
              "Number of Cached Partitions": 0,
              "Memory Size": 0,
              "ExternalBlockStore Size": 0,
              "Disk Size": 0
            }
          ],
          "Parent IDs": [0],
          "Details": "details",
          "Submission Time": 1437323864337,
          "Accumulables":[]
        },
        "Properties": {},
        "appId": "local-1437323845748"
      }
      """
    )
  }

  test("on event - SparkListenerStageCompleted") {
    val stageInfo = new StageInfo(
      stageId = 1,
      attemptId = 0,
      name = "count at <console>:26",
      numTasks = 4,
      rddInfos = Seq.empty,
      parentIds = Seq(0),
      details = "details")

    val stageCompleted = SparkListenerStageCompleted(stageInfo)
    val relay = new TestJsonRelay("local")
    relay.onEvent(stageCompleted)

    checkJson(
      relay.getStreamContent,
      """
      {
        "Event": "SparkListenerStageCompleted",
        "Stage Info": {
          "Stage ID": 1,
          "Stage Attempt ID": 0,
          "Stage Name": "count at <console>:26",
          "Number of Tasks": 4,
          "RDD Info": [],
          "Parent IDs": [0],
          "Details": "details",
          "Accumulables":[]
        },
        "appId": "local"
      }
      """)
  }

  test("on event - SparkListenerExecutorMetricsUpdate, without metrics") {
    val metricsUpdate = SparkListenerExecutorMetricsUpdate(
      execId = "execId",
      taskMetrics = Seq.empty)

    val relay = new TestJsonRelay("local")
    relay.onEvent(metricsUpdate)
    relay.getStreamContent should be ("")
  }

  test("on event - SparkListenerExecutorMetricsUpdate, with metrics") {
    val metricsUpdate = SparkListenerExecutorMetricsUpdate(
      execId = "execId",
      taskMetrics = (110L, 2, 0, TaskMetrics.empty) :: Nil)

    val relay = new TestJsonRelay("local")
    relay.onEvent(metricsUpdate)
    checkJson(
      relay.getStreamContent,
      """
      {
        "Event": "SparkListenerExecutorMetricsUpdate",
        "Executor ID": "execId",
        "Metrics Updated": [
          {
            "Task ID": 110,
            "Stage ID": 2,
            "Stage Attempt ID": 0,
            "Task Metrics": {
              "Host Name": null,
              "Executor Deserialize Time": 0,
              "Executor Run Time": 0,
              "Result Size": 0,
              "JVM GC Time": 0,
              "Result Serialization Time": 0,
              "Memory Bytes Spilled": 0,
              "Disk Bytes Spilled": 0
            }
          }
        ],
        "appId": "local"
      }
      """)
  }

  // `SparkListenerBlockUpdated` is dropped in json relay for now
  test("on event - SparkListenerBlockUpdated") {
    val blockInfo = BlockUpdatedInfo(
      blockManagerId = BlockManagerId("executor-1", "host", 8080),
      blockId = StreamBlockId(streamId = 1, uniqueId = 123L),
      storageLevel = StorageLevel.MEMORY_AND_DISK,
      memSize = 1024L,
      diskSize = 1024L,
      externalBlockStoreSize = 0L)

    val blockUpdated = SparkListenerBlockUpdated(blockInfo)

    val relay = new TestJsonRelay("local")
    relay.onEvent(blockUpdated)
    relay.getStreamContent should be ("")
  }

  test("number of requests and writer content") {
    val appEnd = SparkListenerApplicationEnd(time = 1L)
    val jobEnd1 = SparkListenerJobEnd(jobId = 123, time = 1L, jobResult = JobSucceeded)
    val jobEnd2 = SparkListenerJobEnd(jobId = 124, time = 1L,
      jobResult = JobFailed(new Exception("test")))

    val relay = new TestJsonRelay("local")
    relay.onEvent(appEnd)
    relay.onEvent(jobEnd1)
    relay.onEvent(jobEnd2)

    relay.numReqs should be (3)
    relay.lastEvent.isDefined should be (true)
    relay.getStreamContent.isEmpty should be (false)
  }

  test("send last event when socket failed") {
    val relay = new TestJsonRelay("local")
    relay.setCorruptSocket()

    val appEnd = SparkListenerApplicationEnd(time = 1L)
    relay.onEvent(appEnd)

    checkJson(
      relay.getStreamContent,
      """
      {
        "Event": "SparkListenerApplicationEnd",
        "Timestamp": 1,
        "appId": "local"
      }
      """)
    relay.lastEvent.isDefined should be (true)
  }
}
