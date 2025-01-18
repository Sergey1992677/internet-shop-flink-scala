package configs

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

trait InternetStoreContext {

  lazy val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  lazy val jobName = "InternetStore"
}
