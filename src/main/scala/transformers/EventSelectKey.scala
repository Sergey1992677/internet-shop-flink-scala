package transformers

import dataCaseClasses.{Event, ItemKey}
import org.apache.flink.api.java.functions.KeySelector

class EventSelectKey extends KeySelector[Event, ItemKey] {

  override def getKey(value: Event): ItemKey = ItemKey(value.item_id, value.behavior)
}