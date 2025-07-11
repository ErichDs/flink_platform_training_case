package part2datastreams

import generators.shopping._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{CountTrigger, PurgingTrigger}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Triggers {

  // Triggers -> WHEN a window function is executed

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  def demoCountTrigger(): Unit = {
    val shoppingCartEvents: DataStream[String] = env
      .addSource(new ShoppingCartEventsGenerator(500, 2))           // 2 events/second
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5))) // 10 events/window
      .trigger(CountTrigger.of[TimeWindow](5))                      // the window function runs every 5 elements
      .process(new CountByWindowAll)                                // runs twice for the same window

    shoppingCartEvents.print()
    env.execute()
  }
  /*
    6> Window [1752256195000 - 1752256200000] 10
    7> Window [1752256200000 - 1752256205000] 10
    8> Window [1752256205000 - 1752256210000] 10
    9> Window [1752256210000 - 1752256215000] 10
    10> Window [1752256215000 - 1752256220000] 10

    with trigger
    8> Window [1752257475000 - 1752257480000] 5
    9> Window [1752257480000 - 1752257485000] 5 <- trigger running on the window 80000-85000 for the first time
    10> Window [1752257480000 - 1752257485000] 10 <- second trigger FOR THE SAME WINDOW
    11> Window [1752257485000 - 1752257490000] 5
    12> Window [1752257485000 - 1752257490000] 10
    13> Window [1752257490000 - 1752257495000] 5
   */

  // purging trigger - clear the window when it fires

  def demoPurgingTrigger(): Unit = {
    val shoppingCartEvents: DataStream[String] = env
      .addSource(new ShoppingCartEventsGenerator(500, 2))           // 2 events/second
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5))) // 10 events/window
      .trigger(
        PurgingTrigger.of(CountTrigger.of[TimeWindow](5))
      )                              // the window function runs every 5 elements, THEN CLEARS THE WINDOW
      .process(new CountByWindowAll) // runs twice for the same window

    shoppingCartEvents.print()
    env.execute()
  }
  /*
    with purging trigger
    5> Window [1752257880000 - 1752257885000] 5
    6> Window [1752257880000 - 1752257885000] 5
    7> Window [1752257885000 - 1752257890000] 5
    8> Window [1752257885000 - 1752257890000] 5
    9> Window [1752257890000 - 1752257895000] 5
   */

  /*
    Other triggers:
     - EventTimeTrigger - happens by default when the watermark is > window end time (automatic for event time windows)
     - ProcessingTimeTrigger - fires when the current system time > window end time (automatic for processing time windows)
     - Custom Triggers - powerful APIs for custom firing behavior
   */

  def main(args: Array[String]): Unit =
    demoPurgingTrigger()

}

// copied from Time Based Transformations
class CountByWindowAll extends ProcessAllWindowFunction[ShoppingCartEvent, String, TimeWindow] {
  override def process(context: Context, elements: Iterable[ShoppingCartEvent], out: Collector[String]): Unit = {
    val window = context.window
    out.collect(s"Window [${window.getStart} - ${window.getEnd}] ${elements.size}")
  }
}
