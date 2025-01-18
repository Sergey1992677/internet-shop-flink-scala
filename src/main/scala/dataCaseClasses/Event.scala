package dataCaseClasses

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty

import scala.annotation.meta.field

final case class Event(
                        @(JsonProperty @field)("user_id") user_id: Int,
                        @(JsonProperty @field)("item_id") item_id: Int,
                        @(JsonProperty @field)("category_id") category_id: Int,
                        @(JsonProperty @field)("behavior") behavior: String,
                        @(JsonProperty @field)("ts") ts: String) {

  def this() {
    this(0, 0, 0, "", "")
  }
}
