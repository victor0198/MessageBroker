package Broker

import Broker.MessageBroker.messagesDistributor
import SharedStructures.Message
import SharedStructures.Connection
import SharedStructures.Confirmation
import SharedStructures.Start
import SharedStructures.CloseSocket
import SharedStructures.SendNext
import Utilities.Serialization.SerializeObject
import akka.actor.typed.*
import akka.actor.typed.scaladsl.*

import java.io.{BufferedReader, ByteArrayInputStream, ByteArrayOutputStream, File, InputStream, InputStreamReader, ObjectInputStream, ObjectOutputStream, PrintStream, PrintWriter}
import java.net.{ServerSocket, Socket}
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.{Base64, UUID}
import akka.actor.{Actor, ActorRef, ActorSystem, PoisonPill, Props}

import language.postfixOps
import scala.concurrent.duration.*
import akka.event.{Logging, LoggingAdapter}

import java.util


case object AskForMessage
case object NoMessage
case object CheckSocket
case object Listen

//object MBUtils {
//  def SerializeObject(o: Object): String = {
//    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
//    val oos = new ObjectOutputStream(stream)
//    oos.writeObject(o)
//    oos.close()
//    new String(
//      Base64.getEncoder().encode(stream.toByteArray),
//      StandardCharsets.UTF_8
//    )
//  }
//}



class MessagesHolder(messagesDistributor: ActorRef) extends Actor {
  val messages = new ConcurrentLinkedQueue[Message]()
  val log: LoggingAdapter = Logging(context.system, this)
  def receive: Receive = {
    case message: Message =>
//      log.info("1")
      messages.add(message)
      log.info("MHS: " + messages.size())
      messagesDistributor ! message
//    case AskForMessage =>
//      log.info("2")
//      if(messages.size()>0){
//        sender() ! messages.peek()
//      }else{
//        sender() ! NoMessage
//      }
    case delMgs: DeleteMessage =>
//      log.info("3")
      messages.remove(delMgs.msg)
  }
}

//class ReceiverTicks(messageReceiver: ActorRef) extends Actor {
//  def receive = {
//    case Start =>
//      while(true) {
//        Thread.sleep(20)
//        messageReceiver ! AskForMessage
//      }
//  }
//}


class MessagesDistributor() extends Actor {
  val consumers = new ConcurrentLinkedQueue[CustomerSubscription]()
  val log: LoggingAdapter = Logging(context.system, this)

  def receive: Receive = {
    case msg: Message =>
//      log.info("4")
      log.info("Message:" + msg.topic)

      consumers.forEach(consumer => {
        log.info("consumer topics: " + consumer.topics.mkString("Array(", ", ", ")"))
        consumer.topics.foreach(topic =>{
          if (msg.topic.equals(topic)){
            log.info("sending it!")
            log.info(consumer.sender.toString)
//            consumer.sender ! AskForMessage//msg
            consumer.sender ! Message(msg.id, msg.timeStamp, msg.priority, msg.topic, msg.value)
          }
        })
      })

      sender() ! DeleteMessage(msg)

    case NoMessage =>
//      log.info("5")
      log.info("NO message (from holder)")

    case cc: CustomerSubscription =>
//      log.info("6")
      consumers.add(cc)

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
              val consumerSender = context.actorOf(Props(classOf[ConsumerMessagesSender], sock, sender), "consumerSender:"+consumerIdx)
              val consumerReceiver = context.actorOf(Props(classOf[ConsumerMessageReceiver], is, consumerSender), "consumerReceiver:"+consumerIdx)
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
            log.info("Message: id:" + msg.id + ", topic:" + msg.topic + ", value:" + msg.value)
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
//    case Start =>
//      log.info("8")
//
//      while(true){
//        log.info("b")
//        Thread.sleep(10)
//        if(is.ready()) {
//          val input = is.readLine
//
//          val bytes = Base64.getDecoder.decode(input.getBytes(StandardCharsets.UTF_8))
//
//          val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
//          ois.readObject match {
//            case msg: Message =>
//              log.info("Message: id:" + msg.id + ", topic:" + msg.topic + ", value:" + msg.value)
//              messagesHolder ! msg
//            //                  messages.add(msg)
//            //                  val confirmationMessage = MBUtils.SerializeObject(new Confirmation(msg.id))
//            //                  producer.ps.println(confirmationMessage)
//            case con: Connection =>
//              if(con.subject.equals("disconnect")) {
//                sock.close()
//                log.info("terminating ProducerMessageReceiving actor " + akka.serialization.Serialization.serializedActorPath(self))
//                self ! PoisonPill
//                context.stop(self)
//              }
//            case _ => throw new Exception("Got not a message from client")
//          }
//          ois.close()
//
//        }
//      }

  }
}


class ConsumerMessagesSender(sock: Socket, sen: ActorRef) extends Actor{

  val messages = new ConcurrentLinkedQueue[MessageToSend]()
  val log: LoggingAdapter = Logging(context.system, this)
  var noMsg = 0

  override def preStart(): Unit = log.info("ConsumerMessagesSender starting!")

  override def postStop(): Unit = log.info("ConsumerMessagesSender stopping!")

  def receive: Receive = {
    case AskForMessage =>
//      log.info("YES")
      if(messages.size()>0) {
//        log.info("X")
        var m = new Message("", 0, 0, "", 0)
        val iter = messages.iterator()
        var sent = false
        while(iter.hasNext && !sent){
          val nextMsg = iter.next()
          if(System.nanoTime()-100000000>=nextMsg.lastSentAt){
            m = nextMsg.msg
            log.info("IN LIST: " + messages.size())
            sent = true
          }
        }
        if(sent) {
          log.info("SEnt:" + m.id)
          sen ! m
        }

      }else {
        log.info("No more messages at sender..")
      }
    case msg: Message =>
//      log.info("9")
      var exists = false
      messages.forEach(m=>{
        if(m.msg.id.equals(msg.id) && m.msg.timeStamp.equals(msg.timeStamp)){
          log.info("Removing::"+msg.id)
          messages.remove(m)
          exists = true
        }
      })
      if(!exists) {
        log.info("Adding::" + msg.id)
        messages.add(new MessageToSend(msg, lastSentAt = System.nanoTime()))
        log.info("CMSS: " + messages.size())
        sen ! msg
      }

    case CloseSocket =>
      if(messages.size()>0){
        self ! AskForMessage
      }else{
        sock.close()
        self ! PoisonPill
        sen ! PoisonPill
      }
      self ! CloseSocket



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

class ConsumerMessageReceiver(is: BufferedReader, consumerSender: ActorRef) extends Actor{

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

//          if(input.equals("saturated")){
//            consumer.sock.close()
//            consumers.remove(consumer)
//
//          }else{
          val bytes = Base64.getDecoder.decode(input.getBytes(StandardCharsets.UTF_8))

          val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
          ois.readObject match {
            case con: Connection =>
              if(con.subject.equals("disconnect")) {
                log.info("terminating ProducerMessageReceiving actor " + akka.serialization.Serialization.serializedActorPath(self))
                self ! PoisonPill
                consumerSender ! CloseSocket
                //              context.stop(self)
              }
            case confirm: Confirmation =>

              log.info("got response" + confirm.m.id)

              if(noMsg - noResponsePrev == 0){
                if(replyTiming>30)
                  replyTiming = 20
                else
                  replyTiming = replyTiming - 1
              }else if(noMsg - noResponsePrev == 1 || noMsg - noResponsePrev == 2){
                replyTiming += 3
              }else
                replyTiming += 5

              if(replyTiming<1)
                replyTiming = 1
              log.info("RT" + replyTiming.toString)

              noResponsePrev = noMsg

              consumerSender ! confirm.m

            case _ => throw new Exception("Got not a message from client")
          }
          ois.close()

      }else{
        log.info("no response" + noMsg)
        noMsg += 1
        if(noMsg - noResponsePrev > 10)
          replyTiming = 200

        consumerSender ! AskForMessage
      }
  }
}



object MessageBroker extends App{
  val ss = new ServerSocket(4444)

  val brokerSystem = ActorSystem("messageBroker")

  val messagesDistributor = brokerSystem.actorOf(Props[MessagesDistributor](), "messagesDistributor")

  val messagesHolder = brokerSystem.actorOf(Props(classOf[MessagesHolder], messagesDistributor), "messagesHolder")

  val connectionAccepter = brokerSystem.actorOf(Props(classOf[ConnectionAccepter], ss, messagesHolder, messagesDistributor), "connectionAccepter")
  connectionAccepter ! Start

}
