package deserializers

import dataCaseClasses.Event
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.json.JsonMapper

class EventDeserializer extends DeserializationSchema[Event] {

  private val mapper = JsonMapper
    .builder()
    .build()

  override def deserialize(message: Array[Byte]): Event = {

    mapper.readValue(message, classOf[Event])
  }

  override def isEndOfStream(nextElement: Event): Boolean = false

  implicit val typeInfo: TypeInformation[Event] = TypeInformation.of(classOf[Event])

  override def getProducedType: TypeInformation[Event] = implicitly[TypeInformation[Event]]
}
