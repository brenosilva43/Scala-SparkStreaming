import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.KafkaUtils

object Teste extends App{
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  val spark = SparkSession
    .builder()
    .master("local")
    .appName("teste")
    .getOrCreate()
  val sc = spark.sparkContext
  val ssc = new StreamingContext(sc,Seconds(1))

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "use_a_separate_group_id_for_each_stream",
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val topics = Array("test")
  val stream = KafkaUtils.createDirectStream[String, String](
    ssc,
    PreferConsistent,
    Subscribe[String, String](topics, kafkaParams)
  )

  stream.map(record => (record.value)).print()

  //stream.map(record => (record.key, record.value))
  ssc.start()
  ssc.awaitTermination()


}
