package Producer

import SharedStructures.Message
import SharedStructures.Connection
import SharedStructures.Start
import Utilities.Serialization.SerializeObject
import akka.actor.{Actor, ActorRef, ActorSystem, PoisonPill, Props}

import java.io.{BufferedReader, FileNotFoundException, InputStreamReader, PrintStream}
import java.net.Socket
import java.util.Properties
import scala.io.Source
import akka.event.Logging

class ProduceMessages(ps: PrintStream, sock: Socket, topics: Array[String], messagesPerSecond: Int) extends Actor {
  override def preStart(): Unit = println("Producer - Connecting")

//  override def postStop(): Unit = println("ProduceMessages stopping!")
  val log = Logging(context.system, this)

  def receive = {
    case Start =>
      println("Producer - sending " + messagesPerSecond + " messages..")
      var message_id = 0
      while(message_id < messagesPerSecond){
        message_id += 1

        var topic = ""
        val r = scala.util.Random.nextFloat()
        val value = 50 + (50*r).toInt

        topic = topics(message_id % topics.length)

        var priority = 0
        if(r>0.2)
          priority += 1
        if(r>0.4)
          priority += 1
        if(r>0.6)
          priority += 1
        if(r>0.8)
          priority += 1

        val now = System.nanoTime()
        val message = SerializeObject(new Message(message_id, now, priority, topic, value))
        ps.println(message)
      }
      val message = SerializeObject(new Connection("disconnect", Array[String]()))
      ps.println(message)
      sock.close()
      println("Producer -  connection closed")
      self ! PoisonPill

  }

}

@main def Producer: Unit = {
  Thread.sleep(10000)
  while(true){

    val host = "172.18.0.1"
    val port = 4444
    val sock = new Socket(host, port)
    val is = new BufferedReader(new InputStreamReader(sock.getInputStream))
    val os = new PrintStream(sock.getOutputStream)

    val connectionMessage = SerializeObject(new Connection("producer", Array[String]()))
    os.println(connectionMessage)

    val producerSystem = ActorSystem("producer")

    val properties: Properties = new Properties()
    val source = Source.fromFile("src/main/scala/Producer/producer.properties")
    properties.load(source.bufferedReader())

    val topics = properties.getProperty("topics")
    val messagesPerSecond = properties.getProperty("messagesPerSecond").toInt

    val produceMessages = producerSystem.actorOf(Props(classOf[ProduceMessages], os, sock, topics.split(","), messagesPerSecond), "producer")
    produceMessages ! Start

    Thread.sleep(1000)
  }

}
