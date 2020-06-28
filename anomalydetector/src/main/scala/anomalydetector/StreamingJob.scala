package anomalydetector

import java.util.Properties

import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema

object StreamingJob {
  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //    var executionConfig = env.getConfig.setParallelism(1)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "test")

    val source_clicks = env
      .addSource(new FlinkKafkaConsumer[ObjectNode](
        "clicks",
        new JSONKeyValueDeserializationSchema(false),
        properties))

    val source_displays = env
      .addSource(new FlinkKafkaConsumer[ObjectNode](
        "displays",
        new JSONKeyValueDeserializationSchema(false),
        properties))


    //    source_displays
    //      .writeAsText("./src/main/data/displays.txt")
    //      .setParallelism(1)
    //
    //    source_clicks
    //      .writeAsText("./src/main/data/clicks.txt")
    //      .setParallelism(1)


    source_displays
      .union(source_clicks)
      .keyBy(new KeySelector[ObjectNode, String] {
        override def getKey(in: ObjectNode): String = in.get("value").get("ip").asText()
      })

      .process(new ClickThroughDetector)
      .writeAsText("./src/main/data/ctr_ip_time.txt")
      .setParallelism(1)

    source_displays
      .union(source_clicks)
      .keyBy(new KeySelector[ObjectNode, String] {
        override def getKey(in: ObjectNode): String = in.get("value").get("uid").asText()
      })

      .process(new ClickThroughDetector)
      .writeAsText("./src/main/data/ctr_uid_time.txt")
      .setParallelism(1)
    //        .print


    source_clicks
      .keyBy(new KeySelector[ObjectNode, String] {
        override def getKey(in: ObjectNode): String = in.get("value").get("ip").asText()
      })
      .process(new TimingDetector)
      .writeAsText("./src/main/data/clicks_ip_time.txt")
      .setParallelism(1)
    //        .print

    source_clicks.keyBy(new KeySelector[ObjectNode, String] {
      override def getKey(in: ObjectNode): String = in.get("value").get("uid").asText()
    })
      .process(new TimingDetector)

      .writeAsText("./src/main/data/clicks_uid_time.txt")
      .setParallelism(1)
    //        .print

    env.execute("AnomalyDetector")
  }
}

// TODO : Please delete text files before running Job




