import Consumer.getClass

import java.io.{BufferedInputStream, BufferedOutputStream, BufferedReader, ByteArrayOutputStream, FileNotFoundException, FileOutputStream, InputStreamReader, ObjectOutputStream, PrintStream}
import java.net.{ServerSocket, Socket}
import java.nio.charset.StandardCharsets
import java.util.{Base64, Properties, UUID}
import java.util.concurrent.ConcurrentLinkedQueue
import scala.io.{Source, StdIn}

case class Producer(sock: Socket, is: BufferedReader, ps: PrintStream, name: String)

class ProducerMessageGeneratingThread(sock: Socket, os: PrintStream) extends Thread
{
  override def run()
  {
    println("Messages generating thread - started.")
    Thread.sleep(2000)
    var i = 0
    while(i < 4){
      var topic = ""
      var value = 22
      if (i%2 == 0){
        topic = "temperature"
        value += i/2
      }else{
        topic = "humidity"
        value += 30 + i
      }
      val msg = new Message(UUID.randomUUID().toString, topic, value)
      println("Sending:" + msg.id)
      val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(stream)
      oos.writeObject(msg)
      oos.close()
      val retv = new String(
        Base64.getEncoder().encode(stream.toByteArray),
        StandardCharsets.UTF_8
      )
      os.println(retv)
      Thread.sleep(1000)
      i += 1
    }
    os.println("quit")
    sock.close()
    println("Connection closed")
  }
}


class ProducerMessagesReceiveThread(is: BufferedReader) extends Thread
{
  override def run()
  {
    println("Messages receiving thread - started.")
    var i = 0
    while(i < 3){
      if(is.ready){
        val output = is.readLine
        println("Received: " + output)
        i += 1
      }

      Thread.sleep(100)
    }
  }
}

object Producer {
  def main(args: Array[String]) {
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
    os.println(clientType)

    val messagesReceiveThreadThread = new ProducerMessagesReceiveThread(is)
    messagesReceiveThreadThread.start()

    val messageGeneratingThread = new ProducerMessageGeneratingThread(sock, os)
    messageGeneratingThread.start()

  }
}
