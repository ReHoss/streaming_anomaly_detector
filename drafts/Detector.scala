package my_package

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.walkthrough.common.entity.Alert

//object Detector {
//  val SMALL_AMOUNT: Double = 1.00
//  val LARGE_AMOUNT: Double = 500.00
//  val ONE_MINUTE: Long = 60 * 1000L
//}
//
//@SerialVersionUID(1L)
//class Detector extends KeyedProcessFunction[String, ObjectNode, Alert] {
//
//  @throws[Exception]
//  def processElement(
//                      click: ObjectNode,
//                      context: KeyedProcessFunction[String, ObjectNode, Alert]#Context,
//                      collector: Collector[Alert]): Unit = {
//
//    val alert = new Alert
//    val id = click.get("value").get("ip").asText.replaceAll("[.]", "").toLong
//
//    alert.setId(id)
//    collector.collect(alert)
//  }
//}

object Detector {
  val SMALL_AMOUNT: Double = 1.00
  val LARGE_AMOUNT: Double = 500.00
  val ONE_MINUTE: Long     = 2 * 1000L
  val MAPPER = new ObjectMapper()

}

@SerialVersionUID(1L)
class Detector extends KeyedProcessFunction[String, ObjectNode, String] {

  @transient private var flagState: ValueState[java.lang.Boolean] = _
  @transient private var timerState: ValueState[java.lang.Long] = _

  @throws[Exception]
  override def open(parameters: Configuration): Unit = {
    val flagDescriptor = new ValueStateDescriptor("flag", Types.BOOLEAN)
    flagState = getRuntimeContext.getState(flagDescriptor)

    val timerDescriptor = new ValueStateDescriptor("timer-state", Types.LONG)
    timerState = getRuntimeContext.getState(timerDescriptor)

  }

  override def processElement(
//                               transaction: Transaction,
                               click: ObjectNode,
                               context: KeyedProcessFunction[String, ObjectNode, String]#Context,
                               collector: Collector[String]): Unit = {

    // Get the current state for the current key
    //    val lastTransactionWasSmall = flagState.value

    // Timer start
    val timer = context.timerService.currentProcessingTime + Detector.ONE_MINUTE
    context.timerService.registerProcessingTimeTimer(timer)
    timerState.update(timer)

    if (flagState.value == null) {
      flagState.update(true)
      val timer = context.timerService.currentProcessingTime + Detector.ONE_MINUTE
      context.timerService.registerProcessingTimeTimer(timer)
      timerState.update(timer)
    }
    else if (flagState.value == true) {

      val timer = context.timerService.currentProcessingTime + Detector.ONE_MINUTE
      context.timerService.registerProcessingTimeTimer(timer)
      timerState.update(timer)

      val alert = new Alert
      val id = click.get("value").get("ip").asText.replaceAll("[.]", "").toLong
      alert.setId(id)
//      collector.collect(alert)


      val node = Detector.MAPPER.writeValueAsString(click.get("value"))

      collector.collect(s"Alert : $node")
    }

    // Check if the flag is set
    //    if (lastTransactionWasSmall != null) {
    //      if (transaction.getAmount > FraudDetector.LARGE_AMOUNT) {
    //        // Output an alert downstream
    //        val alert = new Alert
    //        alert.setId(transaction.getAccountId)
    //        collector.collect(alert)
    //      }
    //      // Else Clean up our state
    //      cleanUp(context)
    //    }

    //    if (transaction.getAmount < FraudDetector.SMALL_AMOUNT) {
    //      // set the flag to true
    //      flagState.update(true)
    //      val timer = context.timerService.currentProcessingTime + FraudDetector.ONE_MINUTE
    //
    //      context.timerService.registerProcessingTimeTimer(timer)
    //      timerState.update(timer)
    //    }
    //
  }

  override def onTimer(
                        timestamp: Long,
                        ctx: KeyedProcessFunction[String, ObjectNode, String]#OnTimerContext,
                        out: Collector[String]): Unit = {
    // remove flag after 1 minute
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



