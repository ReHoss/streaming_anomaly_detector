package anomalydetector

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

object TimingDetector {
  val TOTAL_TIME: Long = 2 * 1000L
  val MAPPER = new ObjectMapper()
}

@SerialVersionUID(1L)
class TimingDetector extends KeyedProcessFunction[String, ObjectNode, String] {

  @transient private var flagState: ValueState[java.lang.Boolean] = _
  @transient private var timerState: ValueState[java.lang.Long] = _

  @throws[Exception]
  override def open(parameters: Configuration): Unit = {
    val flagDescriptor = new ValueStateDescriptor("flag", Types.BOOLEAN)
    flagState = getRuntimeContext.getState(flagDescriptor)

    val timerDescriptor = new ValueStateDescriptor("timer-state", Types.LONG)
    timerState = getRuntimeContext.getState(timerDescriptor)

  }

  override def processElement(click: ObjectNode,
                              context: KeyedProcessFunction[String, ObjectNode, String]#Context,
                              collector: Collector[String]): Unit = {

    // Timer start
    val timer = context.timerService.currentProcessingTime + TimingDetector.TOTAL_TIME
    context.timerService.registerProcessingTimeTimer(timer)
    timerState.update(timer)

    if (flagState.value == null) {
      flagState.update(true)
      val timer = context.timerService.currentProcessingTime + TimingDetector.TOTAL_TIME
      context.timerService.registerProcessingTimeTimer(timer)
      timerState.update(timer)
    }
    else if (flagState.value == true) {

      val timer = context.timerService.currentProcessingTime + TimingDetector.TOTAL_TIME
      context.timerService.registerProcessingTimeTimer(timer)
      timerState.update(timer)

      // Collect anormal entry
      val node = TimingDetector.MAPPER.writeValueAsString(click.get("value"))
      collector.collect(s"Alert : $node")
    }
  }

  override def onTimer(
                        timestamp: Long,
                        ctx: KeyedProcessFunction[String, ObjectNode, String]#OnTimerContext,
                        out: Collector[String]): Unit = {
    // remove flag after timer reached
    timerState.clear()
    flagState.clear()
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
