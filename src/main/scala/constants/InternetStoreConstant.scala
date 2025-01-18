package constants

object InternetStoreConstant {

  val targetBehaviorSet: Set[String] = Set("buy", "pv")

  val sourceName = "Kafka-Source"

  val topicTag = "user_behavior"

  val serverName = "localhost:9092"

  val hoursToSubtract = 1L

  val stateName = "itemsWithTime"

  val indexForElasticName = "kafka_data"

  val timeIntervalToGetStats = 10L

  val httpHost = "127.0.0.1"

  val httpPort = 9200

  val httpScheme = "http"

  val numMaxActions = 1

  val flushInterval = 1000L
}
