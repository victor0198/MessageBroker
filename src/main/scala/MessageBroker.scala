import sun.jvm.hotspot.utilities.ObjectReader

import java.io.{BufferedInputStream, BufferedOutputStream, BufferedReader, ByteArrayInputStream, ByteArrayOutputStream, FileInputStream, FileOutputStream, InputStream, InputStreamReader, ObjectInput, ObjectInputStream, ObjectOutputStream, PrintStream}
import java.net.{ServerSocket, Socket}
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.{Base64, UUID}
import java.nio.charset.StandardCharsets

class AcceptConnectionsThread(var ss: ServerSocket, producers: ConcurrentLinkedQueue[Producer], consumers: ConcurrentLinkedQueue[Consumer]) extends Thread
{
  override def run()
  {
    println("Connection accepting thread - started.")
    while(true){
      val sock = ss.accept()
      val is = new BufferedReader(new InputStreamReader(sock.getInputStream()))
      val os = new PrintStream(sock.getOutputStream()) //new PrintStream(new BufferedOutputStream(sock.getOutputStream()))
      val uuid = UUID.randomUUID().toString
      println("Client connected: " + uuid.substring(0,4))

      val clientType = is.readLine
      if(clientType.equals("producer")){
        producers.add(Producer(sock, is, os, uuid))
        println("Producer connected")
      }else if (clientType.equals("consumer")){
        val topics = is.readLine
        consumers.add(Consumer(sock, is, os, uuid, topics))
        println("Consumer connected")
      }

    }
  }
}

class AcceptMessagesThread(producers: ConcurrentLinkedQueue[Producer], consumers: ConcurrentLinkedQueue[Consumer], messages: ConcurrentLinkedQueue[Message]) extends Thread
{
  override def run()
  {
    println("Messages accepting thread - started.")
    while(true){
      producers.forEach((producer) => {
        if(producer.is.ready) {
          val input = producer.is.readLine

          if(input.equals("quit")){
            producer.sock.close()
            producers.remove(producer)

          }else{
            val bytes = Base64.getDecoder.decode(input.getBytes(StandardCharsets.UTF_8))

            val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
            ois.readObject match {
              case msg: Message =>
                println("Message: id:" + msg.id + ", topic:" + msg.topic + ", value:" + msg.value)
                messages.add(msg)
                producer.ps.println("SUCCESS")
              case _ => throw new Exception("Got not a message from client")
            }
            ois.close()
          }
        }
      })
      consumers.forEach((consumer) => {
        if(consumer.is.ready) {
          val input = consumer.is.readLine

          if(input.equals("saturated")){
            consumer.sock.close()
            consumers.remove(consumer)

          }else{
            val bytes = Base64.getDecoder.decode(input.getBytes(StandardCharsets.UTF_8))

            val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
            ois.readObject match {
              case confirm: Confirmation =>
                messages.forEach((message) => {
                  if(message.id.equals(confirm.id)){
                    println("Confirmed:" + message.id)
                    messages.remove(message)
                  }
                })
              case _ => throw new Exception("Got not a message from client")
            }
            ois.close()
          }
        }
      })

      Thread.sleep(100)
    }
  }
}

class MessagesSendingThread(messages: ConcurrentLinkedQueue[Message], consumers: ConcurrentLinkedQueue[Consumer]) extends Thread
{
  override def run()
  {
    println("Messages sending thread - started.")
    while(true) {
      var sendWithPriority = 2
      var prioritySet = false
      while (!prioritySet) {
        messages.forEach((msg) => {
          if (msg.priority == sendWithPriority){
            prioritySet = true
          }
        })

        if(!prioritySet)
          sendWithPriority -= 1

        if(sendWithPriority == 0)
          prioritySet = true
      }

      var messageSent = false
      messages.forEach((msg) => {

        if(!messageSent && consumers.size() > 0 && msg.priority == sendWithPriority){

          val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
          val oos = new ObjectOutputStream(stream)
          oos.writeObject(msg)
          oos.close()
          val retv = new String(
            Base64.getEncoder().encode(stream.toByteArray),
            StandardCharsets.UTF_8
          )

          consumers.forEach((toConsumer) => {
            toConsumer.topics.split(",").foreach(
              (topic)=> {
                if(topic.equals(msg.topic)) {
                  println("Sending  :" + msg.id + "|" + msg.topic + " " + msg.value)
                  Thread.sleep(100)
                  toConsumer.ps.println(retv)
                  messageSent = true
                }
              }
            )
          })
        }
      })

      Thread.sleep(100)
    }
  }
}


class DurableMessagesThread(messages: ConcurrentLinkedQueue[Message]) extends Thread
{
  override def run()
  {
    println("Messages saving thread - started.")
    while(true){

//      val OIS = new ObjectInputStream(new BufferedInputStream(new FileInputStream("messages.bin")))
//      val msgs = OIS.readObject match {
//        case arr: Array[Message] => arr
//        case _ => throw new Exception("Could not get messages from the file")
//      }
//      println("! Messages from the file: " + msgs.toArray.mkString("Array(", ", ", ")"))

//      val messagesArray =  messages.toArray()
//      val MFS = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream("messages.bin")))
//      MFS.writeObject(messagesArray)
//      MFS.close()

      Thread.sleep(3000)
    }
  }
}


object Server {

  def main(args: Array[String]) {
    val producers = new ConcurrentLinkedQueue[Producer]()
    val consumers = new ConcurrentLinkedQueue[Consumer]()
    val messages = new ConcurrentLinkedQueue[Message]()

    val ss = new ServerSocket(4444)

    val connectionsThread = new AcceptConnectionsThread(ss, producers, consumers)
    connectionsThread.start()

    val acceptMessagesThread = new AcceptMessagesThread(producers, consumers, messages)
    acceptMessagesThread.start()

    val messagesSendingThread = new MessagesSendingThread(messages, consumers)
    messagesSendingThread.start()

    val durableMessagesThread = new DurableMessagesThread(messages)
    durableMessagesThread.start()



    Thread.sleep(1000)


    while (true){
      print("\n--Connected producers: ")
      if(producers.size() > 0){
        producers.forEach((producer) => {
          print(producer.name.substring(0,4) + ", ")
        })
      }else{
        print("No one")
      }
      print("\n--Connected consumers: ")
      if(consumers.size() > 0){
        consumers.forEach((consumer) => {
          print(consumer.name.substring(0,4) + ", ")
        })
      }else{
        print("No one")
      }
      print("\n--Arrived messages: ")
      if(messages.size() > 0){
        messages.forEach((msg) => {
          print("(" + msg.topic + ", " + msg.value + ")")
        })
      }else {
        print("No messages")
      }
      println("\n")
      Thread.sleep(3000)
    }
  }

}
