package Broker

import SharedStructures.Message
import SharedStructures.Connection
import SharedStructures.Confirmation
import SharedStructures.Start
import SharedStructures.CloseSocket
import SharedStructures.SendNext
import Utilities.Serialization.SerializeObject
import akka.actor.typed.*
import akka.actor.typed.scaladsl.*

import java.io.{BufferedReader, ByteArrayInputStream, ByteArrayOutputStream, File, FileNotFoundException, InputStream, InputStreamReader, ObjectInputStream, ObjectOutputStream, PrintStream, PrintWriter}
import java.net.{ServerSocket, Socket}
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.{Base64, Properties, UUID}
import akka.actor.{Actor, ActorRef, ActorSystem, PoisonPill, Props}

import language.postfixOps
import scala.concurrent.duration.*
import akka.event.{Logging, LoggingAdapter}

import java.util
import scala.concurrent.Future
import reactivemongo.api.MongoConnection

import scala.io.Source
import scala.util.control.Breaks.break

case object AskForMessage
case object NoMessage
case object CheckSocket
case object Listen
case object AskForConsumers


class MessagesHolder() extends Actor {
  var messages = new util.LinkedList[MessagesPriority]()
  val log: LoggingAdapter = Logging(context.system, this)
  var lastSentAt = 0L

  override def preStart(): Unit = {
    log.info("ProducerMessageReceiving starting!")
    lastSentAt = System.nanoTime()
  }
  def receive: Receive = {
    case message: Message =>
      var added = false
      var it = messages.iterator()
      while(it.hasNext && !added){
        val theList = it.next()
          if(theList.priority == message.priority){
            theList.msgs.add(message)
            added = true
          }
      }

      if(!added) {
        var newArr = new util.LinkedList[Message]()
        newArr.add(message)
        val newType = new MessagesPriority(message.priority, newArr)

        var addedList = false
        it = messages.iterator()
        var index = 0
        while(it.hasNext && !addedList){
          val theList = it.next()
          if(theList.priority < message.priority){
            messages.add(index, newType)
            addedList = true
          }
          index+=1
        }
        if(!addedList)
          messages.addLast(newType)

      }

      it = messages.iterator()
      while(it.hasNext) {
        log.info("A list:")
        val theList = it.next()
        val listIt = theList.msgs.iterator()
        while(listIt.hasNext){
          val item = listIt.next()
          log.info("Message: " + item.id + "|pr" + item.priority)
        }
      }

    case AskForMessage =>
      var sent = false
      val it = messages.iterator()
//      log.info("Getting message from holder")
      while(it.hasNext && !sent){
//        log.info("Checking a list")
        val theList = it.next().msgs
        val oneListIt = theList.iterator()
        while(oneListIt.hasNext && !sent){
          val item = oneListIt.next()
          log.info("Getting from holder:"+item.id)
          sender() ! item
          sent = true
          theList.remove(item)
        }
//        log.info("Common List Size: " + messages.size())
      }

  }
}

class MessagesPropagator(messagesHolder: ActorRef, messagesDistributor: ActorRef) extends Actor {

  var numberOfConsumers = 0
  def receive = {
    case Start =>
      Thread.sleep(100)
//      println("propagator asking")
      messagesDistributor ! AskForMessage

      self ! Start
  }
}


class MessagesDistributor(messagesHolder: ActorRef, maxSentMessagesPerSecond: Int) extends Actor {
  val consumers = new ConcurrentLinkedQueue[CustomerSubscription]()
  val log: LoggingAdapter = Logging(context.system, this)
  var lastTimeAsked = System.nanoTime()

  def receive: Receive = {
    case msg: Message =>
      log.info("got msg" + msg.id)
      consumers.forEach(consumer => {
        consumer.topics.foreach(topic =>{
          if (msg.topic.equals(topic)){
            consumer.sender ! Message(msg.id, msg.timeStamp, msg.priority, msg.topic, msg.value)
          }
        })
      })

    case cs: CustomerSubscription =>
      var exists = false
      consumers.forEach(c =>{
        if(c.sender == cs.sender && cs.topics.length == 0){
          println("removing from consumers list of distributor")
          consumers.remove(c)
          exists = true
        }
      })
      if(!exists)
        consumers.add(cs)

//    case AskForConsumers =>
//      println("sending size:" + consumers.size())
//      sender() ! ConsumersState(consumers.size())

    case AskForMessage =>
//      if((System.nanoTime() - 10000000) > lastTimeAsked &&
      if(!consumers.isEmpty) {
        lastTimeAsked = System.nanoTime()
        Thread.sleep((1000/maxSentMessagesPerSecond).toInt) // 5ms
        messagesHolder ! AskForMessage
        self ! AskForMessage
      }
  }
}


class ConnectionAccepter(ss: ServerSocket, messagesHolder: ActorRef, messagesDistributor: ActorRef) extends Actor {

  val log: LoggingAdapter = Logging(context.system, this)

  def receive: Receive = {
    case Start =>
//      log.info("7")
      log.info("Connection accepting thread - started.")
      var producerIdx = 0
      var consumerIdx = 0
      while(true){
//        log.info("a")
        val sock = ss.accept()
        val is = new BufferedReader(new InputStreamReader(sock.getInputStream))
        val ps = new PrintStream(sock.getOutputStream)
        val uuid = UUID.randomUUID().toString
        log.info("Client connected: " + uuid.substring(0,4))

        val connection = is.readLine
        val bytes = Base64.getDecoder.decode(connection.getBytes(StandardCharsets.UTF_8))

        val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
        ois.readObject match {
          case con: Connection =>
            log.info("Message: client " + con.subject + ", topics:" + con.topics.mkString("Array(", ", ", ")"))
            if(con.subject.equals("producer")){
              log.info("Spawning a producer listener")

              val producerSender = context.actorOf(Props(classOf[ProducerMessageSender], ps, sock), "producerMessageSender:"+producerIdx)
              val producerReceiver = context.actorOf(Props(classOf[ProducerMessageReceiving], is, sock, messagesHolder, producerSender), "producerMessageReceiver:"+producerIdx)
              producerIdx += 1
              producerReceiver ! Listen
            }
            else if (con.subject.equals("consumer")){

              log.info("Spawning a consumer sender")

              val sender = context.actorOf(Props(classOf[Sender], ps), "sender:"+consumerIdx)
              val consumerReceiver = context.actorOf(Props(classOf[ConsumerMessageReceiver], is), "consumerReceiver:"+consumerIdx)
              val consumerSender = context.actorOf(Props(classOf[ConsumerMessagesSender], sock, sender, consumerReceiver, messagesDistributor), "consumerSender:"+consumerIdx)

              consumerReceiver ! AskForMessage
//              val receiverTicks = context.actorOf(Props(classOf[ReceiverTicks], consumerReceiver), "receiverTicks:"+consumerIdx)
//              receiverTicks ! Start

              consumerIdx += 1

              messagesDistributor ! CustomerSubscription(con.topics, consumerSender)
            }
          case _ => throw new Exception("Got not a message from client")
        }
        ois.close()


      }
  }
}

class ProducerMessageReceiving(is: BufferedReader, sock: Socket, messagesHolder: ActorRef, producerSender: ActorRef) extends Actor{

  val log: LoggingAdapter = Logging(context.system, this)


  override def preStart(): Unit = log.info("ProducerMessageReceiving starting!")

  override def postStop(): Unit = log.info("ProducerMessageReceiving stopping!")

  def receive: Receive = {
    case Listen =>
//      log.info("8")

      Thread.sleep(10)
      if(is.ready()) {
        val input = is.readLine

        val bytes = Base64.getDecoder.decode(input.getBytes(StandardCharsets.UTF_8))

        val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
        ois.readObject match {
          case msg: Message =>
            log.info("Message: id:" + msg.id + ", priority:" + msg.priority + ", topic:" + msg.topic + ", value:" + msg.value)
            messagesHolder ! msg
//                  messages.add(msg)
//                  val confirmationMessage = MBUtils.SerializeObject(new Confirmation(msg.id))
//                  producer.ps.println(confirmationMessage)
          case con: Connection =>
            if(con.subject.equals("disconnect")) {
              log.info("terminating ProducerMessageReceiving actor " + akka.serialization.Serialization.serializedActorPath(self))
              self ! PoisonPill
              producerSender ! CloseSocket
//              context.stop(self)
            }
          case _ => throw new Exception("Got not a message from client")
        }
        ois.close()

      }

      producerSender ! CheckSocket


  }
}


class ProducerMessageSender(ps: PrintStream, sock: Socket) extends Actor{

  val log: LoggingAdapter = Logging(context.system, this)


  override def preStart(): Unit = log.info("ProducerMessageSender starting!")

  override def postStop(): Unit = log.info("ProducerMessageSender stopping!")

  def receive: Receive = {
    case CheckSocket =>
      if (!sock.isClosed){
        sender() ! Listen
      }else{
        log.info("producer socket closed!")
      }

    case CloseSocket =>
      sock.close()
      self ! PoisonPill

  }
}


class ConsumerMessagesSender(sock: Socket, sen: ActorRef, consumerReceiver: ActorRef, messagesDistributor: ActorRef) extends Actor{

  val messages = new ConcurrentLinkedQueue[MessageToSend]()
  val log: LoggingAdapter = Logging(context.system, this)
  var noMsg = 0

  override def preStart(): Unit = log.info("ConsumerMessagesSender starting!")

  override def postStop(): Unit = log.info("ConsumerMessagesSender stopping!")

  def receive: Receive = {
    case AskForMessage =>
      if(messages.size()>0) {
        messages.forEach(message =>{
          if(System.nanoTime()-500000000 > message.lastSentAt){
            sen ! message
//            log.info("resending" + message.msg.id)
            consumerReceiver ! AskForMessage
            message.lastSentAt = System.nanoTime()
          }

        })
        self ! AskForMessage
      }

    case msg: Message =>
      log.info("new msg to m sender" + msg.id)
      messages.add(new MessageToSend(msg, lastSentAt = System.nanoTime()))

      sen ! msg

      consumerReceiver ! AskForMessage


    case CloseSocket =>
      sock.close()
      sen ! PoisonPill
      messagesDistributor ! CustomerSubscription(Array[String](), self)
      self ! PoisonPill

    case conf: Confirmation =>
      messages.forEach(m=>{
        if(m.msg.id.equals(conf.m.id) && m.msg.timeStamp.equals(conf.m.timeStamp)){
//          log.info("Removing::"+conf.m.id)
          messages.remove(m)
        }
      })
//      log.info("Sender List Size: " + messages.size())


  }
}



class Sender(ps: PrintStream) extends Actor{

  val log: LoggingAdapter = Logging(context.system, this)
  override def preStart(): Unit = log.info("Sender starting!")

  override def postStop(): Unit = log.info("Sender stopping!")

  def receive: Receive = {

    case msg: Message =>
      val serialized2 = SerializeObject(msg)
      log.info("Sender sending:" + msg.topic + "|" + msg.id)
      ps.println(serialized2)


  }
}

class ConsumerMessageReceiver(is: BufferedReader) extends Actor{

  val log: LoggingAdapter = Logging(context.system, this)
  var replyTiming = 10
  var noMsg = 0
  var noResponsePrev = 0

  override def preStart(): Unit = log.info("ConsumerMessageReceiver starting!")

  override def postStop(): Unit = log.info("ConsumerMessageReceiver stopping!")

  def receive: Receive = {

    case AskForMessage =>
      Thread.sleep(replyTiming)
      if(is.ready) {
        val input = is.readLine

          val bytes = Base64.getDecoder.decode(input.getBytes(StandardCharsets.UTF_8))

          val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
          ois.readObject match {
            case confirm: Confirmation =>
              if (noMsg - noResponsePrev == 0) {
                if (replyTiming > 30)
                  replyTiming = 20
                else
                  replyTiming = replyTiming - 1
              } else if (noMsg - noResponsePrev == 1 || noMsg - noResponsePrev == 2)
                replyTiming += 3
              else
                replyTiming += 5
              if (replyTiming < 1)
                replyTiming = 1
              noResponsePrev = noMsg

              sender() ! confirm

              if(!confirm.connection) {
                log.info("terminating ConsumerMessageReceiving actor " + akka.serialization.Serialization.serializedActorPath(self))
                sender() ! CloseSocket
                self ! PoisonPill
              }
            case _ => throw new Exception("Got not a message from client")
          }
          ois.close()

      }else{
//        log.info("no response" + noMsg)
        noMsg += 1
        if(noMsg - noResponsePrev > 10)
          replyTiming = 200

        sender() ! AskForMessage
      }
  }
}



object MessageBroker extends App{
  val url = getClass.getResource("broker.properties")
  val properties: Properties = new Properties()
  if (url != null) {
    val source = Source.fromURL(url)
    properties.load(source.bufferedReader())
  }
  else {
    println("properties file cannot be loaded")
    throw new FileNotFoundException("Properties file cannot be loaded")
  }

  val maxSentMessagesPerSecond = properties.getProperty("maxSentMessagesPerSecond").toInt

  val ss = new ServerSocket(4444)

  val brokerSystem = ActorSystem("messageBroker")

  val messagesHolder = brokerSystem.actorOf(Props[MessagesHolder](), "messagesHolder")

  val messagesDistributor = brokerSystem.actorOf(Props(classOf[MessagesDistributor], messagesHolder, maxSentMessagesPerSecond), "messagesDistributor")

  val messagesPropagator = brokerSystem.actorOf(Props(classOf[MessagesPropagator], messagesHolder, messagesDistributor), "messagesPropagator")
  messagesPropagator ! Start

  val connectionAccepter = brokerSystem.actorOf(Props(classOf[ConnectionAccepter], ss, messagesHolder, messagesDistributor), "connectionAccepter")
  connectionAccepter ! Start

}
