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
import java.util.{Base64, Properties, UUID}
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean
import scala.io.AnsiColor._
import scala.io.Source

class ConsumeMessages(acceptMessages: Int, is: BufferedReader, ps: PrintStream, sock: Socket, manager: ActorRef) extends Actor
{

  override def preStart(): Unit = log.info("Consumer - Connecting!")

  override def postStop(): Unit = {
    log.info("A consumer stopped")
    manager ! Start
  }
  val log = Logging(context.system, this)

  val receivedMessages = new ConcurrentLinkedQueue[Message]()
  def receive = {
    case Start =>
      val start = System.nanoTime()
      while(receivedMessages.size()<acceptMessages){
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
          if(!exists){
            receivedMessages.add(msgo)
            println(s"${MAGENTA}Consumer - Got message: id " + msgo.id + ", priority " + msgo.priority + ", topic " + msgo.topic + ", value " + msgo.value + s"${RESET}")
          }else{
            println("Consumer - Already existing!")
          }
          val confirmationMessage = new Confirmation(msgo, true)
          if(receivedMessages.size() == acceptMessages)
            confirmationMessage.connection = false
          ps.println(SerializeObject(confirmationMessage))

        }
        Thread.sleep(3)
      }
      sock.close()
      println("Disconnecting")
      self ! PoisonPill
  }
}

class ConsumerManager() extends Actor {

  val log = Logging(context.system, this)

  val receivedMessages = new ConcurrentLinkedQueue[Message]()

  def receive = {
    case Start =>
      val host = "172.18.0.1"
      val port = 4444
      val sock = new Socket(host, port)
      val is = new BufferedReader(new InputStreamReader(sock.getInputStream))
      val ps = new PrintStream(sock.getOutputStream)
      
      val properties: Properties = new Properties()
      val source = Source.fromFile("src/main/scala/Consumer/consumer.properties")
      properties.load(source.bufferedReader())

      val topics = properties.getProperty("topics")
      val acceptMessages = 5000
      val consumerId = UUID.randomUUID().toString
      val connectionMessage = SerializeObject(new Connection(consumerId,"consumer", topics.split(",")))
      ps.println(connectionMessage)

      val producerSystem = ActorSystem("consumer")

      val produceMessages = producerSystem.actorOf(Props(classOf[ConsumeMessages], acceptMessages, is, ps, sock, self), "consumer")
      produceMessages ! Start

  }
}

@main def Consumer: Unit = {
  Thread.sleep(5000)
  val producerSystem = ActorSystem("consumer")

  val produceMessages = producerSystem.actorOf(Props[ConsumerManager](), "consumerManager")
  produceMessages ! Start
  
  while(true){
    Thread.sleep(60000)
  }
}
