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

class ProduceMessages(ps: PrintStream, sock: Socket, topics: Array[String]) extends Actor {
  override def preStart(): Unit = log.info("ProduceMessages starting!")

  override def postStop(): Unit = log.info("ProduceMessages stopping!")
  val log = Logging(context.system, this)
  
  def receive = {
    case Start =>

      println("Messages generating thread - started.")
      var message_id = 0
      while(message_id < 200){
        message_id += 1

        var topic = ""
        val r = scala.util.Random.nextFloat()
        val value = 50 + (50*r).toInt

        topic = topics(message_id % topics.length)

        var priority = 0
        if(r>0.2)
          priority += 1
//        if(r>0.4)
//          priority += 1
//        if(r>0.6)
//          priority += 1
//        if(r>0.8)
//          priority += 1

//        println("Sending: priority " + priority + " | "+ topic + " " + value)
        val now = System.nanoTime()
        val message = SerializeObject(new Message(message_id, now, priority, topic, value))
        ps.println(message)
//        log.info("message sent")
//        Thread.sleep(5)
      }
      val message = SerializeObject(new Connection("disconnect", Array[String]()))
      ps.println(message)
      sock.close()
      println("Connection closed")
      self ! PoisonPill

  }

}

object Producer extends App{
//  while(true){

    val host = "localhost"
    val port = 4444
    val sock = new Socket(host, port)
    val is = new BufferedReader(new InputStreamReader(sock.getInputStream))
    val os = new PrintStream(sock.getOutputStream)

    val connectionMessage = SerializeObject(new Connection("producer", Array[String]()))
    os.println(connectionMessage)

    val producerSystem = ActorSystem("producer")


    val url = getClass.getResource("producer.properties")
    val properties: Properties = new Properties()
    if (url != null) {
      val source = Source.fromURL(url)
      properties.load(source.bufferedReader())
    }
    else {
      println("properties file cannot be loaded")
      throw new FileNotFoundException("Properties file cannot be loaded")
    }

    val topics = properties.getProperty("topics")

    val produceMessages = producerSystem.actorOf(Props(classOf[ProduceMessages], os, sock, topics.split(",")), "producer")
    produceMessages ! Start

//    Thread.sleep(1000)
//  }

}
