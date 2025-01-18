import configs.InternetStoreContext
import transformers.InternetStoreAnalyser

object InternetStoreApp extends InternetStoreContext {

  def main(args: Array[String]): Unit = {

    val internetStoreAnalyser = new InternetStoreAnalyser(env)

    internetStoreAnalyser.sendStatsToElastic()

    env.execute(jobName)
  }
}
