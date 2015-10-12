# JsonRelay
[`JsonRelay`][] is a [`SparkListener`][] that converts [`SparkListenerEvent`][]s to JSON and forwards them to an external service via RPC.

It is designed to be used with [`slim`][], which consumes JsonRelay's emitted events and writes useful statistics about them to Mongo, from whence [Spree][] serves up live-updating web pages.

## Usage
With Spark >= 1.5.0 you can simply pass the following flags to your `spark-shell` and `spark-submit` commands:

```sh
    --packages org.hammerlab:spark-json-relay:2.0.1
    --conf spark.extraListeners=org.apache.spark.JsonRelay
```

If using earlier versions of Spark, you'll need to first download the JAR:

```
$ wget https://repo1.maven.org/maven2/org/hammerlab/spark-json-relay/2.0.1/spark-json-relay-2.0.1.jar
```

Then, pass these flags to your `spark-submit` or `spark-shell` commands:
```
    --driver-class-path spark-json-relay-2.0.1.jar
    --conf spark.extraListeners=org.apache.spark.JsonRelay
```

That's it!

Two additional flags, `--conf spark.slim.{host,port}`, specify the location `JsonRelay` will attempt to connect and send events to.

## Implementation

`JsonRelay` just piggybacks on Spark's [`JsonProtocol`][] for JSON serialization, with two differences:

1. It adds an `appId` field to all events; this allows downstream consumers to process events from multiple Spark applications simultaneously / more easily over time.
2. It rolls its own serialization of `SparkListenerExecutorMetricsUpdate` events, which is [omitted from Spark's `JsonProtocol`](https://github.com/apache/spark/blob/v1.4.1/core/src/main/scala/org/apache/spark/util/JsonProtocol.scala#L96) in Spark prior to `1.5.0` (cf. [SPARK-9036][]).

## Questions?

Please file an issue if you have any questions about or problems using `JsonRelay`!

[`slim`]: https://github.com/hammerlab/slim
[Spree]: https://github.com/hammerlab/spree
[`JsonRelay`]: https://github.com/hammerlab/spark-json-relay/blob/abfea947334a6185cfd43e64a552806094c4c584/client/src/main/scala/org/apache/spark/JsonRelay.scala
[`SparkListener`]: https://github.com/apache/spark/blob/658814c898bec04c31a8e57f8da0103497aac6ec/core/src/main/scala/org/apache/spark/scheduler/SparkListener.scala#L137
[`SparkListenerEvent`]: https://github.com/apache/spark/blob/658814c898bec04c31a8e57f8da0103497aac6ec/core/src/main/scala/org/apache/spark/scheduler/SparkListener.scala#L32-L128
[SPARK-9036]: https://issues.apache.org/jira/browse/SPARK-9036
[`JsonProtocol`]: https://github.com/apache/spark/blob/v1.4.1/core/src/main/scala/org/apache/spark/util/JsonProtocol.scala
