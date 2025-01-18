package readStream

import dataCaseClasses.Event
import deserializers.EventDeserializer
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

class KafkaReader(env: StreamExecutionEnvironment) {

  def readStream(serverName: String,
                 topicTag: String,
                 sourceName: String): DataStreamSource[Event] = {

    val kafkaSource = KafkaSource.builder[Event]()
      .setBootstrapServers(serverName)
      .setTopics(topicTag)
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(new EventDeserializer())
      .build()

    env.fromSource(
      kafkaSource,
      WatermarkStrategy.noWatermarks(),
      sourceName)
  }
}
