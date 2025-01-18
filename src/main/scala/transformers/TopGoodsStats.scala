package transformers

import constants.InternetStoreConstant
import dataCaseClasses._
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

import java.lang
import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter

import scala.jdk.CollectionConverters._

class TopGoodsStats extends ProcessWindowFunction[Event, RecordForElastic, ItemKey, GlobalWindow] {

  private var itemsWithTimeState: ValueState[Iterable[TimeOfBehavior]] = _

  override def open(parameters: Configuration): Unit = {

    itemsWithTimeState = getRuntimeContext.getState(

      new ValueStateDescriptor[Iterable[TimeOfBehavior]](
        InternetStoreConstant.stateName,
        classOf[Iterable[TimeOfBehavior]], Iterable[TimeOfBehavior]())
    )
  }

  override def process(key: ItemKey,
                       ctx: ProcessWindowFunction[Event, RecordForElastic, ItemKey, GlobalWindow]#Context,
                       elements: lang.Iterable[Event],
                       out: Collector[RecordForElastic]): Unit = {

    val lastHourTime = LocalDateTime
      .now()
      .minusHours(InternetStoreConstant.hoursToSubtract)

    val dtf = DateTimeFormatter
      .ofPattern("yyyy-MM-dd HH:mm:ss")
      .withZone(ZoneId.of("Europe/Moscow"))

    val events = TopGoodsStats.getItemsWithTime(elements, dtf) ++ itemsWithTimeState.value()

    val eventsWithinLastHour = events
      .filter(record => record.ts.isAfter(lastHourTime))

    val record = RecordForElastic(key.itemId, key.behavior, eventsWithinLastHour.size)

    itemsWithTimeState.update(eventsWithinLastHour)

    out.collect(record)
  }
}

private object TopGoodsStats {

  private val getItemsWithTime = (elements: lang.Iterable[Event], dtf: DateTimeFormatter) => {

    elements
      .asScala
      .map(record => {

        val eventDateTime = LocalDateTime.parse(record.ts, dtf)

        TimeOfBehavior(eventDateTime)
      })
  }
}
