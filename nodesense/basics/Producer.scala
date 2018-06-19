package ai.nodesense.basics

import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.util.Random


import org.joda.time._;
import play.api.libs.json._;


object Producer extends App {


  def runProducer() = {
    val events = 10;
    val topic = "measurements"
    val brokers = "localhost:9092"
    val rnd = new Random()
    val props = new Properties()

    props.put("bootstrap.servers", brokers)
    props.put("client.id", "ScalaProducer")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")


    val producer = new KafkaProducer[String, String](props)
    val t = System.currentTimeMillis()
    for (nEvents <- Range(0, events)) {
      val runtime = new Date().getTime()
      val ip = "192.168.2." + rnd.nextInt(255)
      val msg = runtime + "," + nEvents + ",www.example.com," + ip
      val data = new ProducerRecord[String, String](topic, ip, msg)

      //async
      //producer.send(data, (m,e) => {})
      //sync

      //producer.send(data)
    }

    var eventId = 0;

    val thread = new Thread {
      override def run {
        while (true) {
          // your custom behavior here
          Thread.sleep(2000);
          println("by thread");

          val runtime = new Date().getTime()
          val ip = "192.168.2." + rnd.nextInt(255)
          var msg = runtime + "," + eventId + ",www.example.com," + ip

          implicit val modelFormat = Json.format[Voltage]

          val voltage = Voltage("sensor1", rnd.nextInt(100), new Date().getTime());

          val jsValue:JsValue = Json.toJson(voltage)

          msg = jsValue.toString

          val data = new ProducerRecord[String, String](topic, ip, msg)
          eventId += 1;
          //async
          //producer.send(data, (m,e) => {})
          //sync
          producer.send(data)
        }

      }
    };

    thread.start();

    System.out.println("sent per second: " + events * 1000 / (System.currentTimeMillis() - t))
    // producer.close()
  }

  runProducer();
}