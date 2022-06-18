package Broker

import SharedStructures.{Ack, CloseSocket, Confirmation, Connection, Message, PubAck, Publish, SendNext, Start}
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
import scala.io.AnsiColor._

case object AskForMessage
case object NoMessage
case object CheckSocket
case object Listen
case object AskForConsumers
case object AskForSave


class MessagesHolder() extends Actor {
  var messages = new util.LinkedList[MessagesPriority]()
  val log: LoggingAdapter = Logging(context.system, this)
  var lastSentAt = 0L

  override def preStart(): Unit = {
    log.info(s"${REVERSED}${WHITE}Reading messages from Persistent Storage" + s"${RESET}")
    lastSentAt = System.nanoTime()
    val file = Source.fromFile("SavedMessages/Messages.txt")
    val it = file.getLines()
    while(it.hasNext){
      val line = it.next()
      if(line.startsWith("~")){
        val newArr = new util.LinkedList[Message]()
        val newType = new MessagesPriority(line.substring(1,2).toInt, newArr)
        messages.addLast(newType)
      }else{
        val thisLine = line.split("[|]")
        messages.getLast.msgs.add(Message(thisLine(0).toInt, "", thisLine(1).toLong, thisLine(2).toInt, thisLine(3), thisLine(4).toInt))
      }
    }

    file.close()

  }
  def receive: Receive = {
    case message: Message =>
      log.info(s"${REVERSED}${WHITE}Adding message to queue: id " + message.id + ", priority " + message.priority + ", topic " + message.topic + ", value " + message.value + s"${RESET}")
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
        val newArr = new util.LinkedList[Message]()
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

    case AskForMessage =>
      var sent = false
      val it = messages.iterator()
      while(it.hasNext && !sent){
        val theList = it.next().msgs
        val oneListIt = theList.iterator()
        while(oneListIt.hasNext && !sent){
          val item = oneListIt.next()
          sender() ! item
          sent = true
          theList.remove(item)
        }
      }

    case AskForSave =>
      log.info(s"${REVERSED}${WHITE}Saving queued messages" + s"${RESET}")
      val file = new File("SavedMessages/Messages.txt")
      val pw = new PrintWriter(file)

      val it = messages.iterator()
      while(it.hasNext){
        val elem = it.next()
        val oneListIt = elem.msgs.iterator()
        pw.write("~" + elem.priority + "\n")
        while(oneListIt.hasNext){
          val item = oneListIt.next()
          pw.write(item.id.toString + "|" + item.timeStamp + "|" + item.priority + "|" + item.topic + "|" + item.value + "\n")
        }
      }

      pw.close()
  }
}

class MessagesSaving(messagesHolder: ActorRef, saveMessagesInterval: Int) extends Actor {

  val log: LoggingAdapter = Logging(context.system, this)
  def receive = {
    case Start =>
      Thread.sleep(1000 * saveMessagesInterval)
      messagesHolder ! AskForSave
      self ! Start
  }
}

class MessagesPropagator(messagesHolder: ActorRef, messagesDistributor: ActorRef, maxSentMessagesPerSecond: Int) extends Actor {
  val log: LoggingAdapter = Logging(context.system, this)

  def receive = {
    case Start =>
      Thread.sleep(100)
      messagesDistributor ! AskForMessage

      self ! Start
  }
}


class MessagesDistributor(messagesHolder: ActorRef, producerAcknowledger: ActorRef) extends Actor {
  val consumers = new ConcurrentLinkedQueue[CustomerSubscription]()
  val log: LoggingAdapter = Logging(context.system, this)
  var lastTimeAsked = System.nanoTime()

  def receive: Receive = {
    case msg: Message =>
      var consumersNumber = 0
      consumers.forEach(consumer => {
        consumer.topics.foreach(topic =>{
          if (msg.topic.equals(topic)){
            consumer.sender ! msg
            consumersNumber += 1
          }
        })
      })
      if(!consumers.isEmpty)
        producerAcknowledger ! BrokerAck(msg.producerId ,msg.id, consumersNumber, 0)

    case cs: CustomerSubscription =>
      var exists = false
      consumers.forEach(c =>{
        if(c.sender == cs.sender && cs.topics.length == 0){
          consumers.remove(c)
          exists = true
        }
      })
      if(!exists)
        consumers.add(cs)

    case AskForMessage =>
      if(!consumers.isEmpty) {
        lastTimeAsked = System.nanoTime()
        Thread.sleep((1000/100).toInt)
        messagesHolder ! AskForMessage
      }

  }
}


class ConnectionAccepter(ss: ServerSocket, messagesHolder: ActorRef, messagesDistributor: ActorRef, producerAcknowledge: ActorRef) extends Actor {

  val log: LoggingAdapter = Logging(context.system, this)

  def receive: Receive = {
    case Start =>
      var producerIdx = 0
      var consumerIdx = 0
      while(true){
        val sock = ss.accept()
        val is = new BufferedReader(new InputStreamReader(sock.getInputStream))
        val ps = new PrintStream(sock.getOutputStream)

        val connection = is.readLine
        val bytes = Base64.getDecoder.decode(connection.getBytes(StandardCharsets.UTF_8))

        val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
        ois.readObject match {
          case con: Connection =>
            log.info(s"${REVERSED}${RED}New client: " + con.subject + ", topics:" + con.topics.mkString("Array(", ", ", ")") + s"${RESET}")
            if(con.subject.equals("producer")){
              val producerSender = context.actorOf(Props(classOf[ProducerMessageSender], ps, sock), "producerMessageSender:"+producerIdx)
              val producerReceiver = context.actorOf(Props(classOf[ProducerMessageReceiving], is, sock, con.clientId, messagesHolder, producerSender, producerAcknowledge), "producerMessageReceiver:"+producerIdx)

              producerAcknowledge ! ProducerInfo(con.clientId, producerSender)
              producerReceiver ! Listen
              producerIdx += 1
            }
            else if (con.subject.equals("consumer")){
              val sender = context.actorOf(Props(classOf[ConsumerMessagesSender], ps), "ConsumerMessagesSender:"+consumerIdx)
              val consumerReceiver = context.actorOf(Props(classOf[ConsumerMessageReceiver], is, producerAcknowledge), "ConsumerMessageReceiver:"+consumerIdx)
              val consumerSender = context.actorOf(Props(classOf[ConsumerMessagesManager], sock, sender, consumerReceiver, messagesDistributor), "ConsumerMessagesManager:"+consumerIdx)

              consumerReceiver ! AskForMessage
              messagesDistributor ! CustomerSubscription(con.topics, consumerSender)
              consumerIdx += 1
            }
          case _ => throw new Exception("Got not a message from client")
        }
        ois.close()
      }
  }
}

class ProducerMessageReceiving(is: BufferedReader, sock: Socket, producerId: String, messagesHolder: ActorRef, producerSender: ActorRef, producerAcknowledge:ActorRef) extends Actor{

  val log: LoggingAdapter = Logging(context.system, this)
  var publishedItems = util.LinkedList[Publish]()

  def receive: Receive = {
    case Listen =>
      Thread.sleep(5)
      if(is.ready()) {
        val input = is.readLine

        val bytes = Base64.getDecoder.decode(input.getBytes(StandardCharsets.UTF_8))

        val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
        ois.readObject match {
          case publish: Publish =>
            if(publish.qos==0){
              messagesHolder ! publish.message
            }else if(publish.qos==1) {

              // check if message is duplicate
              var sameMessage = false
              publishedItems.forEach(pub => {
                if (pub.message.producerId == publish.message.producerId) {
                  if (pub.message.id == publish.message.id) {
                    sameMessage = true
                  }
                }
              })

              if (sameMessage) {
                log.info(s"${REVERSED}${BLUE}Got PUBLISH DUP" + s"${RESET}")
              }

              if (!sameMessage) {
                // delete previous message
                if (!publishedItems.isEmpty) {
                  var sameProducerMessage = false
                  var toRemove = publishedItems.get(0)
                  var continueRemoving = true
                  while (continueRemoving) {
                    publishedItems.forEach(pub => {
                      if (pub.message.producerId == publish.message.producerId) {
                        sameProducerMessage = true
                        toRemove = pub
                      }
                    })
                    if (sameProducerMessage) {
                      publishedItems.remove(toRemove)
                    } else {
                      continueRemoving = false
                    }
                    sameProducerMessage = false
                  }
                }

                log.info(s"${REVERSED}${BLUE}Got PUBLISH" + s"${RESET}")
                messagesHolder ! publish.message

                producerSender ! new PubAck(publish.message.id)
                publishedItems.add(publish)
              }
            }

          case con: Connection =>
            if(con.subject.equals("disconnect")) {
              sock.close()
              producerSender ! PoisonPill
              producerAcknowledge ! producerId
              self ! PoisonPill
            }

          case _ => throw new Exception("Got not a message from client")
        }
        ois.close()
      }
      if (!sock.isClosed){
        self ! Listen
      }
  }
}

class ProducerAcknowledge() extends Actor{

  val log: LoggingAdapter = Logging(context.system, this)
  val producers = util.LinkedList[ProducerInfo]()
  val acknowledgements = util.LinkedList[BrokerAck]

  def receive: Receive = {
    case producerInfo: ProducerInfo =>
      producers.add(producerInfo)

    case ack: BrokerAck =>
//      log.info(s"${REVERSED}${YELLOW}ACK: to producer:"+ack.producerId + " | Message id:" + ack.messageId)
      acknowledgements.add(ack)

    case confirmation: Confirmation =>
      var removeAck = false
      var ackToRemove = acknowledgements.get(0)
      acknowledgements.forEach(anAck => {
        if(confirmation.m.id==anAck.messageId && confirmation.m.producerId==anAck.producerId){
          anAck.confirmationsReceived+=1
        }
        if(anAck.confirmationsReceived == anAck.confirmationsNeeded){
          removeAck = true
          ackToRemove = anAck
        }
      })
      log.info(s"${REVERSED}${MAGENTA}Confirmation from consumer to producer("+confirmation.m.producerId+") on message: " + confirmation.m.id + s"${RESET}")

      producers.forEach(theProducer => {
        if(theProducer.producerId.equals(ackToRemove.producerId)){
          theProducer.producerSender ! new BrokerAck(ackToRemove.producerId, ackToRemove.messageId, ackToRemove.confirmationsNeeded, ackToRemove.confirmationsReceived)
        }
      })
      acknowledgements.remove(ackToRemove)

    case producerId: String =>
      val it = producers.iterator()
      var producerToRemove = producers.get(0)
      while(it.hasNext){
        val producer = it.next()
        if(producer.producerId.equals(producerId)){
          producerToRemove=producer
        }
      }
      producers.remove(producerToRemove)
  }
}

class ProducerMessageSender(ps: PrintStream, sock: Socket) extends Actor{

  val log: LoggingAdapter = Logging(context.system, this)

  def receive: Receive = {
    case ack: BrokerAck =>
      val serializedAck = SerializeObject(new Ack(ack.messageId))
      ps.println(serializedAck)
      log.info(s"${REVERSED}${GREEN}ACK: to producer:"+ack.producerId + " | Message id:" + ack.messageId + s"${RESET}")
    case puback: PubAck =>
      log.info(s"${REVERSED}${YELLOW}Sending PUBACK. Message:"+puback.messageId + s"${RESET}")
      val serializedPubAck = SerializeObject(puback)
      ps.println(serializedPubAck)
  }
}


class ConsumerMessagesManager(sock: Socket, sender: ActorRef, consumerReceiver: ActorRef, messagesDistributor: ActorRef) extends Actor{

  val messages = new ConcurrentLinkedQueue[MessageToSend]()
  val log: LoggingAdapter = Logging(context.system, this)
  var noMsg = 0

  def receive: Receive = {
    case AskForMessage =>
      if(messages.size()>0) {
        messages.forEach(message =>{
          if(System.nanoTime()-200000000 > message.lastSentAt && !sock.isClosed){
//            sender ! message
            consumerReceiver ! AskForMessage
            message.lastSentAt = System.nanoTime()
          }

        })
        self ! AskForMessage
      }

    case msg: Message =>
      messages.add(new MessageToSend(msg, lastSentAt = System.nanoTime()))
      sender ! msg
      consumerReceiver ! AskForMessage

    case CloseSocket =>
      sock.close()
      sender ! PoisonPill
      messagesDistributor ! CustomerSubscription(Array[String](), self)
      self ! PoisonPill
    case conf: Confirmation =>
      messages.forEach(m=>{
        if(m.msg.id.equals(conf.m.id) && m.msg.timeStamp.equals(conf.m.timeStamp)){
          messages.remove(m)
        }
      })
  }
}



class ConsumerMessagesSender(ps: PrintStream) extends Actor{

  val log: LoggingAdapter = Logging(context.system, this)
  def receive: Receive = {

    case message: Message =>
      val serialized2 = SerializeObject(message)
      log.info(s"${REVERSED}${MAGENTA}Sending to consumer: id " + message.id + ", priority " + message.priority + ", topic " + message.topic + ", value " + message.value + s"${RESET}")
      ps.println(serialized2)

  }
}

class ConsumerMessageReceiver(is: BufferedReader, producerAcknowledge: ActorRef) extends Actor{

  val log: LoggingAdapter = Logging(context.system, this)
  var replyTiming = 10
  var noMsg = 0
  var noResponsePrev = 0

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
              producerAcknowledge ! confirm

              if(!confirm.connection) {
                sender() ! CloseSocket
                self ! PoisonPill
              }
            case _ => throw new Exception("Got not a message from client")
          }
          ois.close()

      }else{
        noMsg += 1
        if(noMsg - noResponsePrev > 10)
          replyTiming = 200

        sender() ! AskForMessage
      }
  }
}



@main def MessageBroker = {
  val properties: Properties = new Properties()
  val source = Source.fromFile("src/main/scala/Broker/broker.properties")
  properties.load(source.bufferedReader())

  val maxSentMessagesPerSecond = properties.getProperty("maxSentMessagesPerSecond").toInt
  val saveMessagesInterval = properties.getProperty("saveMessagesInterval").toInt

  val ss = new ServerSocket(4444)

  val brokerSystem = ActorSystem("messageBroker")

  val messagesHolder = brokerSystem.actorOf(Props[MessagesHolder](), "messagesHolder")

  val producerAcknowledge = brokerSystem.actorOf(Props[ProducerAcknowledge](), "producerAcknowledge")

  val messagesDistributor = brokerSystem.actorOf(Props(classOf[MessagesDistributor], messagesHolder, producerAcknowledge), "messagesDistributor")

  val messagesSaving = brokerSystem.actorOf(Props(classOf[MessagesSaving], messagesHolder, saveMessagesInterval), "messagesSaving")
  messagesSaving ! Start

  val messagesPropagator = brokerSystem.actorOf(Props(classOf[MessagesPropagator], messagesHolder, messagesDistributor, maxSentMessagesPerSecond), "messagesPropagator")
  messagesPropagator ! Start

  val connectionAccepter = brokerSystem.actorOf(Props(classOf[ConnectionAccepter], ss, messagesHolder, messagesDistributor, producerAcknowledge), "connectionAccepter")
  connectionAccepter ! Start

  while(true){
    Thread.sleep(60000)
  }

}
