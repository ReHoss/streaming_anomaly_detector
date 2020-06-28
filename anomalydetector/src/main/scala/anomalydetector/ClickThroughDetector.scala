package anomalydetector

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

object ClickThroughDetector {
  val TOTAL_TIME: Long = 60 * 60 * 1000L
  val DATA_TRESHOLD: Long = 10
  val RATE_TRESHOLD: Double = 0.1
  val MAPPER = new ObjectMapper()
}

@SerialVersionUID(1L)
class ClickThroughDetector extends KeyedProcessFunction[String, ObjectNode, String] {

  @transient private var flagState: ValueState[java.lang.Boolean] = _
  @transient private var timerState: ValueState[java.lang.Long] = _
  @transient private var displaysCount: ValueState[java.lang.Double] = _
  @transient private var clicksCount: ValueState[java.lang.Double] = _
  @transient private var rate: ValueState[java.lang.Double] = _

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

  override def processElement(event: ObjectNode,
                              context: KeyedProcessFunction[String, ObjectNode, String]#Context,
                              collector: Collector[String]): Unit = {

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

    if ((displaysCount.value != null) && (displaysCount.value >= ClickThroughDetector.DATA_TRESHOLD)) {
      if (clicksCount.value == null) {
        clicksCount.update(0.0)
      }
      rate.update(clicksCount.value / displaysCount.value)

//      println(rate.value())
//      println("display", (displaysCount.value(), event.get("value").get("uid").asText()))
//      println("clicks", (clicksCount.value(), event.get("value").get("uid").asText()))

      // Check rate and collect anomaly if necessary
      if (rate.value > 0.1) {
        // Collect abnormal entry
        val node = ClickThroughDetector.MAPPER.writeValueAsString(event.get("value"))
        collector.collect(s"Alert : $node")
      }
    }

    val timer = context.timerService.currentProcessingTime + ClickThroughDetector.TOTAL_TIME
    context.timerService.registerProcessingTimeTimer(timer)
    timerState.update(timer)
  }

  override def onTimer(
                        timestamp: Long,
                        ctx: KeyedProcessFunction[String, ObjectNode, String]#OnTimerContext,
                        out: Collector[String]): Unit = {
    // remove flag after 1 minute
    timerState.clear()
    flagState.clear()
    displaysCount.clear()
    clicksCount.clear()
    rate.clear()
  }

  @throws[Exception]
  private def cleanUp(ctx: KeyedProcessFunction[String, ObjectNode, String]#Context): Unit = {
    // delete timer
    val timer = timerState.value
    ctx.timerService.deleteProcessingTimeTimer(timer)

    // clean up all states
    timerState.clear()
    flagState.clear()
  }
}



