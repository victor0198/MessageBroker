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

class ProduceMessages(ps: PrintStream, sock: Socket) extends Actor {
  override def preStart(): Unit = log.info("ProduceMessages starting!")

  override def postStop(): Unit = log.info("ProduceMessages stopping!")
  val log = Logging(context.system, this)
  
  def receive = {
    case Start =>


      println("Messages generating thread - started.")
      Thread.sleep(100)
      var x = 0
      while(x<100){
        x+=1

        var topic = ""
        val r = scala.util.Random.nextFloat()
        val value = 50 + (50*r).toInt

        if (x%2==0){
          topic = "entered"
        }else{
          topic = "leaved"
        }

        var priority = 0
        if(r>0.25 && r<0.75){
          priority += 1
        }

        println("Sending: priority " + priority + " | "+ topic + " " + value)
        val now = System.nanoTime()
        val message = SerializeObject(new Message(x.toString, now, priority, topic, value))
        ps.println(message)
        log.info("message sent")

//        messageHolder ! new Message(x.toString, priority, topic, value)


//        if(x%2==0)
//          Thread.sleep(x*2)
      }
      val message = SerializeObject(new Connection("disconnect", Array[String]()))
      ps.println(message)
      sock.close()
      println("Connection closed")
      self ! PoisonPill

  }

}

object Producer extends App{
  while(true){

    val host = "localhost"
    val port = 4444
    val sock = new Socket(host, port)
    val is = new BufferedReader(new InputStreamReader(sock.getInputStream))
    val os = new PrintStream(sock.getOutputStream)

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
    val clientType = properties.getProperty("clientType")
    val connectionMessage = SerializeObject(new Connection(clientType, Array[String]()))
    os.println(connectionMessage)


    val producerSystem = ActorSystem("producer")

    val produceMessages = producerSystem.actorOf(Props(classOf[ProduceMessages], os, sock), "producer")
    produceMessages ! Start

    Thread.sleep(3000)
  }

}
