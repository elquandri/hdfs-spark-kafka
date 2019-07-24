import org.apache.spark.sql.SparkSession
import com.typesafe.config.{Config, ConfigFactory}

trait configuration {


  val config:Config=ConfigFactory.load("Kafka.conf")
  val kafkaProd =config.getString("kafkaProd")
  val kafkaCon =config.getString("kafkaCon")
  val topic=config.getString("topic")
  val appName=config.getString("appName")
  val master =config.getString("master")
  val server =config.getString("server")
  val pathResult =config.getString("pathResult")
  val resultPrefix = config.getString("resultPrefix")
  val pathHdfs = config.getString("pathHdfs")
  val csvFile = config.getString("csvFile")


  // sparkConsumer configuration
  val spark = SparkSession.builder.appName(appName).master(master).getOrCreate()

  // configuration du topic
  val topics = List(topic)


}
