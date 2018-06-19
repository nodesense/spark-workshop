package ai.nodesense.basics


import java.util.concurrent._
import java.util.{Collections, Properties}

import kafka.consumer.KafkaStream
import kafka.utils.Logging
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

import scala.collection.JavaConversions._

import org.joda.time._;
import play.api.libs.json._;

class Consumer(val brokers: String,
                     val groupId: String,
                     val topic: String) extends Logging {

  val props = createConsumerConfig(brokers, groupId)
  val consumer = new KafkaConsumer[String, String](props)
  var executor: ExecutorService = null

  def shutdown() = {
    if (consumer != null)
      consumer.close();
    if (executor != null)
      executor.shutdown();
  }

  def createConsumerConfig(brokers: String, groupId: String): Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props
  }

  def run() = {
    println("consumer running");
    consumer.subscribe(Collections.singletonList(this.topic))

    implicit val modelFormat = Json.format[Voltage]

    Executors.newSingleThreadExecutor.execute(    new Runnable {
      override def run(): Unit = {
        while (true) {
          println("consumer running");
          val records = consumer.poll(1000)

          for (record <- records) {
            val msg:String = record.value()
            val jsonObj = Json.parse(msg)

            val result: JsResult[Voltage] = Json.fromJson[Voltage](jsonObj)

            val voltage: Voltage = result.get

            println("**Voltage ", voltage.sensor, voltage.volt)

            System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset())
          }
        }
      }
    })
  }
}

object Consumer extends App {
  //
  //  val example = new ScalaConsumerExample(args(0), args(1), args(2))
  //  example.run()

  def runConsumer() = {
    val broker:String = "localhost:9092";
    val groupId: String = "group1";
    val topic: String  = "measurements";

    val example = new Consumer(broker, groupId, topic)
    example.run()
  }

  runConsumer();

}