package transformers

import constants.InternetStoreConstant
import dataCaseClasses.{Event, RecordForElastic}
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchSink
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{ContinuousProcessingTimeTrigger, PurgingTrigger}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import readStream.KafkaReader
import writeStream.ElasticStreamBuilder

class InternetStoreAnalyser(env: StreamExecutionEnvironment) {

  def sendStatsToElastic(): Unit = {

    val kafkaReader = new KafkaReader(env)

    val kafkaStream: DataStream[Event] = kafkaReader
      .readStream(
        InternetStoreConstant.serverName,
        InternetStoreConstant.topicTag,
        InternetStoreConstant.sourceName
      )

    val minutes = InternetStoreConstant.timeIntervalToGetStats

    val processTimeTrigger = PurgingTrigger
      .of(ContinuousProcessingTimeTrigger
        .of[GlobalWindow](Time.minutes(minutes)))

    val topBuyAndViewGoodsStream = kafkaStream
      .filter(new BuyAndViewGoods)
      .keyBy(new EventSelectKey)
      .window(GlobalWindows.create)
      .trigger(processTimeTrigger)
      .process(new TopGoodsStats)

    val elasticBuilder = new ElasticStreamBuilder()

    val sink: ElasticsearchSink[RecordForElastic] = elasticBuilder.build()

    topBuyAndViewGoodsStream.sinkTo(sink)
  }
}
