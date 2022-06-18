package Producer

import SharedStructures.{Ack, CloseSocket, Connection, Message, PubAck, Publish, Start}
import Utilities.Serialization.SerializeObject
import akka.actor.{Actor, ActorRef, ActorSystem, PoisonPill, Props}
import akka.event.{Logging, LoggingAdapter}

import java.io.{BufferedReader, ByteArrayInputStream, FileNotFoundException, InputStreamReader, ObjectInputStream, PrintStream}
import java.net.Socket
import java.util.{Base64, Properties, UUID}
import scala.io.Source
import akka.event.Logging

import java.nio.charset.StandardCharsets
import java.util
import scala.io.AnsiColor._

case object Listen

class ProduceMessages(ps: PrintStream, sock: Socket, topics: Array[String], messagesPerSecond: Int, producerId: String, qos: Int) extends Actor {
  override def preStart(): Unit = println("Producer - Connecting")

  val log = Logging(context.system, this)
  var message_id = 0
  var publishItem = util.LinkedList[Publish]()
  def receive = {
    case Start =>
      if(message_id < messagesPerSecond){
        if(publishItem.size()>0){
          println(s"${BLUE}PUBLISH message " + message_id + s" DUP${RESET}")
          val publish = publishItem.get(0)
          publish.dup = true
          val message = SerializeObject(publish)
          ps.println(message)
        }else {
          message_id += 1
          println(s"${BLUE}PUBLISH message " + message_id + s"${RESET}")

          var topic = ""
          val r = scala.util.Random.nextFloat()
          val value = 50 + (50 * r).toInt

          topic = topics(message_id % topics.length)

          var priority = 0
          if (r > 0.2)
            priority += 1
          if (r > 0.4)
            priority += 1
          if (r > 0.6)
            priority += 1
          if (r > 0.8)
            priority += 1

          val now = System.nanoTime()
          val publish = new Publish(new Message(message_id, producerId, now, priority, topic, value), false, qos)
          if(qos>0)
            publishItem.add(publish)
          val message = SerializeObject(publish)
          ps.println(message)
        }
        Thread.sleep(10)
        self ! Start
      }else{
        self ! CloseSocket
      }

    case puback: PubAck =>
      println(s"${YELLOW}PUBACK" + puback.messageId + s"${RESET}")
      if(publishItem.size()>0) {
        var pub = publishItem.get(0)
        publishItem.forEach(onePub => {
          if (pub.message.id == puback.messageId)
            pub = onePub
        })
        publishItem.remove(pub)
      }

    case CloseSocket =>
      Thread.sleep(3000)
      val message = SerializeObject(new Connection(producerId, "disconnect", Array[String]()))
      ps.println(message)
      sock.close()
      println("Producer -  connection closed")
      self ! PoisonPill

  }

}


class AckReceiver(is: BufferedReader, sock: Socket, producerMessages: ActorRef, messagesSent: Int) extends Actor{

  val log: LoggingAdapter = Logging(context.system, this)
  var acks = 0
  //  var acksDisplayed = false

  override def postStop(): Unit = {
    println("Total ACKs received: " + acks + " out of " + messagesSent)
  }

  def receive: Receive = {
    case Listen =>

      Thread.sleep(10)
      if(is.ready()) {
        val input = is.readLine

        val bytes = Base64.getDecoder.decode(input.getBytes(StandardCharsets.UTF_8))

        val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
        ois.readObject match {
          case ack: Ack =>
            acks += 1
            println(s"${GREEN}ACK - message " + ack.messageId + s"${RESET}")
          case pubAck: PubAck =>
            producerMessages ! pubAck

          case _ => throw new Exception("Got not a message from client")
        }
        ois.close()

      }

      if (!sock.isClosed){
        self ! Listen
      }else{
        self ! PoisonPill
      }


  }
}

@main def Producer: Unit = {
  while(true){

    val host = "172.18.0.1"
    val port = 4444
    val sock = new Socket(host, port)
    val is = new BufferedReader(new InputStreamReader(sock.getInputStream))
    val os = new PrintStream(sock.getOutputStream)

    val producerId = UUID.randomUUID().toString
    val connectionMessage = SerializeObject(new Connection(producerId, "producer", Array[String]()))
    os.println(connectionMessage)


    val producerSystem = ActorSystem("producer")
    val properties: Properties = new Properties()
    val source = Source.fromFile("src/main/scala/Producer/producer.properties")
    properties.load(source.bufferedReader())

    val topics = properties.getProperty("topics")
    val qos = properties.getProperty("qos").toInt

    val messagesPerSecond = properties.getProperty("messagesPerBatch").toInt
    val produceMessages = producerSystem.actorOf(Props(classOf[ProduceMessages], os, sock, topics.split(","), messagesPerSecond, producerId, qos), "producer")
    produceMessages ! Start

    val ackReceiver = producerSystem.actorOf(Props(classOf[AckReceiver], is, sock, produceMessages, messagesPerSecond), "ackReceiver")
    ackReceiver ! Listen

    Thread.sleep(5000)
  }

}