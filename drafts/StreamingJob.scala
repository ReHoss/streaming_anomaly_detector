///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package package
//
//import org.apache.flink.streaming.api.scala._
//
///**
// * Skeleton for a Flink Streaming Job.
// *
// * For a tutorial how to write a Flink streaming application, check the
// * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
// *
// * To package your application into a JAR file fo r execution, run
// * 'mvn clean package' on the command line.
// *
// * If you change the name of the main class (with the public static void main(String[] args))
// * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
// */
//object StreamingJob {
//  def main(args: Array[String]) {
//    // set up the streaming execution environment
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//
////    print()
//
//    val text: DataStream[String] = env.readTextFile("test.txt:///src/main/data")
//
//    /*
//     * Here, you can start creating your execution plan for Flink.
//     *
//     * Start with getting some data from the environment, like
//     *  env.readTextFile(textPath);
//     *
//     * then, transform the resulting DataStream[String] using operations
//     * like
//     *   .filter()
//     *   .flatMap()
//     *   .join()
//     *   .group()
//     *
//     * and many more.
//     * Have a look at the programming guide:
//     *
//     * https://flink.apache.org/docs/latest/apis/streaming/index.html
//     *
//     */
//
//    // execute program
//    env.execute("Flink Streaming Scala API Skeleton")
//  }
//}

package my_package

import java.util.Properties

import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema
//import org.apache.flink.walkthrough.common.sink.AlertSink


object StreamingJob {
  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    var executionConfig = env.getConfig.setParallelism(1)


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





    //    TEST HERE
    //
    //    val output = source_displays
    //      .coGroup(source_clicks)
    //      .where(new KeySelector[ObjectNode, String] {
    //                override def getKey(in: ObjectNode): String = in.get("value").get("ip").asText()
    //      })
    //      .equalTo(new KeySelector[ObjectNode, String] {
    //        override def getKey(in: ObjectNode): String = in.get("value").get("ip").asText()
    //      })


    //    source_clicks.print
    //
    //    val connectedStreams = source_displays.connect(source_clicks)
    //
    //    connectedStreams
    //      .keyBy(
    //        new KeySelector[ObjectNode, String] {
    //          override def getKey(in: ObjectNode): String = in.get("value").get("uid").asText()
    //        },
    //        new KeySelector[ObjectNode, String] {
    //          override def getKey(in: ObjectNode): String = in.get("value").get("uid").asText()
    //        })
    //
    //      .process(new Detector_click_rate)
    //      .addSink(new AlertSink)

    //    val connectedStreams = source_displays
    //      .keyBy(
    //        new KeySelector[ObjectNode, String] {
    //          override def getKey(in: ObjectNode): String = in.get("value").get("uid").asText()
    //        })
    //      .connect(
    //        source_clicks.keyBy(
    //          new KeySelector[ObjectNode, String] {
    //            override def getKey(in: ObjectNode): String = in.get("value").get("uid").asText()
    //          }))
    //
    //      .process(new Detector_click_rate)
    //      .addSink(new AlertSink)


    //    source_displays
    //      .map(new RichMapFunction[ObjectNode, (ObjectNode, Int)] {
    //        def map(in: ObjectNode): (ObjectNode, Int) = (in, 1)
    //      })
    //      .keyBy(
    //        new KeySelector[(ObjectNode, Int), String] {
    //          override def getKey(in: (ObjectNode, Int)): String = in._1.get("value").get("uid").asText()
    //        }
    //      )
    //      .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
    //      .reduce(new ReduceFunction[(ObjectNode, Int)] {
    //        override def reduce(t: (ObjectNode, Int), t1: (ObjectNode, Int)): (ObjectNode, Int) = ???
    //      })

//
//    import scala.collection.JavaConversions._
//
//    class OuterJoin extends CoGroupFunction[ObjectNode, ObjectNode,
//      (ObjectNode, Option[ObjectNode])] {
//
////      override def coGroup(iterable: lang.Iterable[ObjectNode], iterable1: lang.Iterable[ObjectNode], collector: Collector[(ObjectNode, Option[ObjectNode])]): Unit = ???
//      override def coGroup(
//                            leftElements: lang.Iterable[ObjectNode],
//                            rightElements: lang.Iterable[ObjectNode],
//                            out: Collector[(ObjectNode, Option[ObjectNode])]): Unit = {
//
//        for (leftElem <- leftElements) {
//          var isMatch = false
//          //          println(leftElem.get("value").get("uid").asText())
//          for (rightElem <- rightElements) {
//            println(rightElem.get("value").get("uid").asText())
//            out.collect((leftElem, Some(rightElem)))
//            isMatch = true
//          }
//          if (!isMatch) {
//            out.collect((leftElem, None))
//          }
//        }
//      }
//    }
//
//    source_displays
//      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ObjectNode](Time.seconds(10))())
//      .coGroup(source_clicks)
//        .where(new KeySelector[ObjectNode, String] {
//                    override def getKey(in: ObjectNode): String = in.get("value").get("uid").asText()})
//      .equalTo(new KeySelector[ObjectNode, String] {
//        override def getKey(in: ObjectNode): String = in.get("value").get("uid").asText()})
//      .window(TumblingEventTimeWindows.of(Time.of(5, TimeUnit.SECONDS)))
//        .apply(new OuterJoin)



//
//    source_clicks
//      .union(source_displays)
//      .keyBy(new KeySelector[ObjectNode, String] {
//              override def getKey(in: ObjectNode): String = in.get("value").get("uid").asText()
//            })
//
//      .process(new Detector_bis)
//      .addSink(new AlertSink)


    // END TEST


    //    source_displays
    //        .writeAsText("./src/main/data/displays.txt")
    //        .setParallelism(1)
    //
    //    source_clicks
    //        .writeAsText("./src/main/data/clicks.txt")
    //        .setParallelism(1)

        source_clicks

          .keyBy(new KeySelector[ObjectNode, String] {
            override def getKey(in: ObjectNode): String = in.get("value").get("ip").asText()
          })

          .process(new Detector)
//            .print()
//          .writeAsText("./src/main/data/tst.txt")
//          .addSink(new AlertSink)




    //    source_clicks.keyBy(new KeySelector[ObjectNode, String] {
    //      override def getKey(in: ObjectNode): String = in.get("value").get("uid").asText()
    //    })
    //
    //      .process(new Detector)
    //      .addSink(new AlertSink)
    //
    env.execute("Window Stream WordCount")



    // TODO: Revoir le timer de Detector
  }
}