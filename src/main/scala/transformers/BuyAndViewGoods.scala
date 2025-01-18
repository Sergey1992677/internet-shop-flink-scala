package transformers

import constants.InternetStoreConstant
import dataCaseClasses.Event
import org.apache.flink.api.common.functions.FilterFunction

class BuyAndViewGoods extends FilterFunction[Event] {

  override def filter(value: Event): Boolean = {

    InternetStoreConstant.targetBehaviorSet.contains(value.behavior)
  }
}
