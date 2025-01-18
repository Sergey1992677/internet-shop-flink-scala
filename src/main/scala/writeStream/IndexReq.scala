package writeStream

import constants.InternetStoreConstant
import dataCaseClasses.RecordForElastic
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.elasticsearch.common.xcontent.XContentType

import scala.jdk.CollectionConverters._

class IndexReq extends Serializable {

  def createIndexRequest(element: RecordForElastic): IndexRequest = {

    val json = Map(
      "itemId" -> element.itemId,
      "behavior" -> element.behavior,
      "behaviorCount" -> element.behaviorCount
    )

    Requests
      .indexRequest
      .index(InternetStoreConstant.indexForElasticName)
      .source(json.asJava, XContentType.JSON)
  }
}
