package my_package

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.walkthrough.common.entity.Alert

object Detector_click_rate {
  val ONE_MINUTE: Long = 60 * 1000L
  val TRESHOLD: Double = 0.1
}

@SerialVersionUID(1L)
class Detector_click_rate extends KeyedCoProcessFunction[String, ObjectNode, ObjectNode, Alert] {

  @transient private var flagState: ValueState[java.lang.Boolean] = _
  @transient private var timerState: ValueState[java.lang.Long] = _
  @transient private var displaysCount: ValueState[java.lang.Double] = _
  @transient private var clicksCount: ValueState[java.lang.Double] = _
  @transient private var rate: ValueState[java.lang.Double] = _

  //  clicks_count.update(0)
  //  rate.update(0)

  @throws[Exception]
  override def open(parameters: Configuration): Unit = {
    val flagDescriptor = new ValueStateDescriptor("flag", Types.BOOLEAN)
    flagState = getRuntimeContext.getState(flagDescriptor)

    val timerDescriptor = new ValueStateDescriptor("timer-state", Types.LONG)
    timerState = getRuntimeContext.getState(timerDescriptor)

    val displaysCountDescriptor = new ValueStateDescriptor("displays-count", Types.DOUBLE)
    displaysCount = getRuntimeContext.getState(displaysCountDescriptor)

    val clicksCountDescriptor = new ValueStateDescriptor("clicks-count", Types.DOUBLE)
    clicksCount = getRuntimeContext.getState(clicksCountDescriptor)

    val rateCountDescriptor = new ValueStateDescriptor("rate", Types.DOUBLE)
    rate = getRuntimeContext.getState(rateCountDescriptor)

  }


  override def processElement1(
                                //                               transaction: Transaction,
                                click: ObjectNode,
                                context: KeyedCoProcessFunction[String, ObjectNode, ObjectNode, Alert]#Context,
                                collector: Collector[Alert]): Unit = {

    if (displaysCount.value == null) {
      displaysCount.update(0.0)
    } else {
      displaysCount.update(displaysCount.value + 1)
      //      println(click)
      //      print(displaysCount.value)
    }

  }

  override def processElement2(
                                //                               transaction: Transaction,
                                click: ObjectNode,
                                context: KeyedCoProcessFunction[String, ObjectNode, ObjectNode, Alert]#Context,
                                collector: Collector[Alert]): Unit = {

    if (clicksCount.value == null) {
      clicksCount.update(0.0)
    } else {
      clicksCount.update(clicksCount.value + 1)
    }

//    println("display:", displaysCount.value, click.get("value").get("uid").asText())
//    println("clicks:", clicksCount.value, click.get("value").get("uid").asText())



    if (displaysCount.value != null) {
      if (displaysCount.value > 10) {
        rate.update(clicksCount.value / displaysCount.value)

        if (rate.value < 0.1) {
          //          println(rate.value)
        }
      }
    }




    //    clicks_count = clicks_count + 1
    //
    //
    //    if (displays_count > 15){
    //
    //      rate = clicks_count / displays_count
    //
    //
    //      if (rate > 0) {
    //        println(rate)
    //      }
    //      if (rate > 0.1){
    //
    //        val alert = new Alert
    //        val id = click.get("value").get("ip").asText.replaceAll("[.]", "").toLong
    //        alert.setId(id)
    //        collector.collect(alert)
    //      }
    //    }

    //    println(click.get("value").get("uid").asText())

  }


  override def onTimer(
                        timestamp: Long,
                        ctx: KeyedCoProcessFunction[String, ObjectNode, ObjectNode, Alert]#OnTimerContext,
                        out: Collector[Alert]): Unit = {
    // remove flag after 1 minute
    timerState.clear()
    flagState.clear()
  }

  @throws[Exception]
  private def cleanUp(ctx: KeyedProcessFunction[String, ObjectNode, Alert]#Context): Unit = {
    // delete timer
    val timer = timerState.value
    ctx.timerService.deleteProcessingTimeTimer(timer)

    // clean up all states
    timerState.clear()
    flagState.clear()
  }


}



