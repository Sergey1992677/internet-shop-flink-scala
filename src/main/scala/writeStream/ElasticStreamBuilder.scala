package writeStream

import constants.InternetStoreConstant
import dataCaseClasses.RecordForElastic
import org.apache.flink.api.connector.sink2.SinkWriter
import org.apache.flink.connector.elasticsearch.sink.{Elasticsearch7SinkBuilder, ElasticsearchSink, RequestIndexer}
import org.apache.http.HttpHost

class ElasticStreamBuilder {

  def build(): ElasticsearchSink[RecordForElastic] ={

    val host = new HttpHost(InternetStoreConstant.httpHost,
      InternetStoreConstant.httpPort,
      InternetStoreConstant.httpScheme)

    new Elasticsearch7SinkBuilder[RecordForElastic]()
      .setBulkFlushInterval(InternetStoreConstant.flushInterval)
      .setBulkFlushMaxActions(InternetStoreConstant.numMaxActions)
      .setHosts(host)
      .setEmitter((element: RecordForElastic, context: SinkWriter.Context, indexer: RequestIndexer) =>

        indexer.add(
          new IndexReq().createIndexRequest(element)))
      .build()
  }
}
