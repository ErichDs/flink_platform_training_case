package part2datastreams

import generators.gaming.{PlayerRegistered, ServerEvent}
import generators.shopping._
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, Watermark, WatermarkGenerator, WatermarkOutput, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Instant

object TimeBasedTransformations {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val shoppingCartEvents: DataStream[ShoppingCartEvent] =
    env.addSource(
      new ShoppingCartEventsGenerator(
        sleepMillisPerEvent = 100,
        batchSize = 5,
        baseInstant = Instant.parse("2022-02-15T00:00:00.000Z")
      )
    )

  // 1. Event time = the moment the event was CREATED
  // 2. Processing time = the moment the event ARRIVES AT FLINK

  class CountByWindowAll extends ProcessAllWindowFunction[ShoppingCartEvent, String, TimeWindow] {
    override def process(
      context: Context,
      elements: Iterable[ShoppingCartEvent],
      out: Collector[String]
    ): Unit = {
      val window = context.window
      out.collect(
        s"Window [${window.getStart} - ${window.getEnd}] ${elements.size}"
      )
    }
  }

  /*
    Group by window, every 3s, tumbling (non-overlapping), PROCESSING TIME
   */
  /*
  With processing time
    - we don't care when the event was created
    - multiple runs generate different results
   */
  def demoProcessingTime(): Unit = {
    def groupedEventsByWindow = shoppingCartEvents.windowAll(
      TumblingProcessingTimeWindows.of(Time.seconds(3))
    )
    def countEventsByWindow: DataStream[String] =
      groupedEventsByWindow.process(new CountByWindowAll)
    countEventsByWindow.print()
    env.execute()
  }

  /*
    With event time
      - we NEED to care about handling late data - done with watermarks
      - we don't care about Flink internal time
      - we might see faster results
      - same events + different runs => same results
   */
  def demoEventTime(): Unit = {
    val groupedEventsByWindow = shoppingCartEvents
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(
            java.time.Duration.ofMillis(500)
          ) // max delay < 500 millis
          .withTimestampAssigner(
            new SerializableTimestampAssigner[ShoppingCartEvent] {
              override def extractTimestamp(
                element: ShoppingCartEvent,
                recordTimestamp: Long
              ): Long = element.time.toEpochMilli
            }
          )
      )
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(3)))

    def countEventsByWindow: DataStream[String] =
      groupedEventsByWindow.process(new CountByWindowAll)
    countEventsByWindow.print()
    env.execute()
  }

  /** Custom watermarks
    */
  // with every new MAX timestamp, every new incoming element with event time < max timestamp - max delay will be discarded
  class BoundedOutOfOrdernessGenerator(maxDelay: Long) extends WatermarkGenerator[ShoppingCartEvent] {
    var currentMaxTimestamp: Long = 0L

    // maybe emit watermark on a particular event
    override def onEvent(
      event: ShoppingCartEvent, // event being processed
      eventTimestamp: Long,     // timestamp attached to the event
      output: WatermarkOutput   // immutable data structure that allows us to push new watermarks for flink to handle later
    ): Unit =
      currentMaxTimestamp = Math.max(currentMaxTimestamp, event.time.toEpochMilli)
    // emitting a watermark is NOT mandatory
    // output.emitWatermark(new Watermark(event.time.toEpochMilli)) // every new event older than THIS EVENT will be discarded

    // Flink can also call onPeriodicEmit regularly - up to us to maybe emit a watermark at these times
    override def onPeriodicEmit(output: WatermarkOutput): Unit =
      output.emitWatermark(new Watermark(currentMaxTimestamp - maxDelay - 1))
  }

  /*
    13> Window [1752179952000 - 1752179955000] 25
    14> Window [1752179955000 - 1752179958000] 30
    15> Window [1752179958000 - 1752179961000] 30
    16> Window [1752179961000 - 1752179964000] 30
    1> Window [1752179964000 - 1752179967000] 30
    2> Window [1752179967000 - 1752179970000] 30
    3> Window [1752179970000 - 1752179973000] 30
    4> Window [1752179973000 - 1752179976000] 30
    5> Window [1752179976000 - 1752179979000] 30
    6> Window [1752179979000 - 1752179982000] 30
   */

  def demoEventTime_v2(): Unit = {
    // control how often Flink calls onPeriodicEmit
    env.getConfig.setAutoWatermarkInterval(
      1000L
    ) // call onPeriodicEmit every 1s

    val groupedEventsByWindow = shoppingCartEvents
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forGenerator(_ => new BoundedOutOfOrdernessGenerator(500L))
          .withTimestampAssigner(
            new SerializableTimestampAssigner[ShoppingCartEvent] {
              override def extractTimestamp(
                element: ShoppingCartEvent,
                recordTimestamp: Long
              ): Long = element.time.toEpochMilli
            }
          )
      )
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(3)))

    def countEventsByWindow: DataStream[String] =
      groupedEventsByWindow.process(new CountByWindowAll)
    countEventsByWindow.print()
    env.execute()
  }

  def main(args: Array[String]): Unit =
    demoEventTime_v2()
}
