package Consumer

import SharedStructures.Connection
import SharedStructures.Message
import SharedStructures.Confirmation
import SharedStructures.Start
import Utilities.Serialization.SerializeObject
import akka.actor.{Actor, ActorRef, ActorSystem, PoisonPill, Props}
import akka.event.Logging

import java.io.{BufferedReader, ByteArrayInputStream, FileNotFoundException, InputStreamReader, ObjectInputStream, PrintStream}
import java.net.Socket
import java.nio.charset.StandardCharsets
import java.util.{Base64, Properties}
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean
import scala.io.Source

class ConsumeMessages(is: BufferedReader, ps: PrintStream, sock: Socket, manager: ActorRef) extends Actor
{

  override def preStart(): Unit = log.info("ConsumeMessages starting!")

  override def postStop(): Unit = {
    log.info("ConsumeMessages stopping!")

    manager ! Start
  }
  val log = Logging(context.system, this)

  val receivedMessages = new ConcurrentLinkedQueue[Message]()
  def receive = {
    case Start =>
      log.info("Messages receiving thread - started.")
      val start = System.nanoTime()
      while(receivedMessages.size()<=5000){
        if(is.ready){
          val input = is.readLine
          val bytes = Base64.getDecoder.decode(input.getBytes(StandardCharsets.UTF_8))

          val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
          val msgo = ois.readObject match {
            case msg: Message => msg
            case _ => throw new Exception("Got not a message from client")
          }
          ois.close()

          var exists = false
          receivedMessages.forEach((msg) => {
            if(msg.id == msgo.id && msg.timeStamp == msgo.timeStamp){
              exists = true
            }
          })
          log.info("Received            : " + msgo.topic + " " + msgo.value + "| id " + msgo.id)
          if(!exists){
            log.info("Adding!")
            receivedMessages.add(msgo)
            log.info(receivedMessages.size().toString)
            log.info("Sending confirmation:" + msgo.id)
            val confirmationMessage = SerializeObject(new Confirmation(msgo))
            ps.println(confirmationMessage)
          }else{
            log.info("Already existing!")

          }

        }
        if(receivedMessages.size()==195){
          println(System.nanoTime() - start)

        }
//        Thread.sleep(3)
      }
      val message = SerializeObject(new Connection("disconnect", Array[String]()))
      ps.println(message)
      sock.close()

      self ! PoisonPill
  }
}

class ConsumerManager() extends Actor {

  override def preStart(): Unit = log.info("ConsumerManager starting!")

  override def postStop(): Unit = log.info("ConsumerManager stopping!")

  val log = Logging(context.system, this)

  val receivedMessages = new ConcurrentLinkedQueue[Message]()

  def receive = {
    case Start =>
      val host = "localhost"
      val port = 4444
      val sock = new Socket(host, port)
      val is = new BufferedReader(new InputStreamReader(sock.getInputStream))
      val ps = new PrintStream(sock.getOutputStream)

      val url = getClass.getResource("consumer.properties")
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
      val valueType = properties.getProperty("valueType")

      val connectionMessage = SerializeObject(new Connection(clientType, valueType.split(",")))
      ps.println(connectionMessage)

      val producerSystem = ActorSystem("consumer")

      val produceMessages = producerSystem.actorOf(Props(classOf[ConsumeMessages], is, ps, sock, self), "consumer")
      produceMessages ! Start

  }
}

object Consumer extends App {
  val producerSystem = ActorSystem("consumer")

  val produceMessages = producerSystem.actorOf(Props[ConsumerManager](), "consumerManager")
  produceMessages ! Start
}
