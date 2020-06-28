package my_package

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.walkthrough.common.entity.Alert

object Detector_bis {
  val TOTAL_TIME: Long = 60 * 1000L
  val DATA_TRESHOLD: Long = 10
}

@SerialVersionUID(1L)
class Detector_bis extends KeyedProcessFunction[String, ObjectNode, Alert] {

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

  override def processElement(
                               //                               transaction: Transaction,
                               event: ObjectNode,
                               context: KeyedProcessFunction[String, ObjectNode, Alert]#Context,
                               collector: Collector[Alert]): Unit = {

    // Get the current state for the current key
    //    val lastTransactionWasSmall = flagState.value

    // Display count update
    if (event.get("value").get("eventType").asText() == "display") {
      if (displaysCount.value == null) {
        displaysCount.update(1.0)

      } else {
        displaysCount.update(displaysCount.value + 1)
      }
    }
    // Clicks count update
    else {
      if (clicksCount.value == null) {
        clicksCount.update(1.0)
      } else {
        clicksCount.update(clicksCount.value + 1)
      }
    }

    // Rate update


    if ((displaysCount.value != null) && (displaysCount.value >= Detector_bis.DATA_TRESHOLD)) {
      if (clicksCount.value == null) {
        clicksCount.update(0.0)
      }
      rate.update(clicksCount.value / displaysCount.value)
//      println(rate.value())
      // Set alert

      if (rate.value > 0.1)
      {val alert = new Alert
      val id = event.get("value").get("ip").asText.replaceAll("[.]", "").toLong
      alert.setId(id)
      collector.collect(alert)}
    }




    //    if ((clicksCount.value != null) & (displaysCount.value != null)){
    //      rate.update(clicksCount.value / displaysCount.value)
    //      println(rate.value())
    //    }


    //    println("display", (displaysCount.value(), event.get("value").get("uid").asText()))
    //    println("clicks", (clicksCount.value(), event.get("value").get("uid").asText()))

    //    if (displaysCount.value > 10) {

    //      if (rate.value < 0.1) {
    //                  println(rate.value)
    //      }
    //    }

    val timer = context.timerService.currentProcessingTime + Detector_bis.TOTAL_TIME
    context.timerService.registerProcessingTimeTimer(timer)
    timerState.update(timer)
  }

  override def onTimer(
                        timestamp: Long,
                        ctx: KeyedProcessFunction[String, ObjectNode, Alert]#OnTimerContext,
                        out: Collector[Alert]): Unit = {
    // remove flag after 1 minute
    timerState.clear()
    flagState.clear()
    displaysCount.clear()
    clicksCount.clear()
    rate.clear()
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



