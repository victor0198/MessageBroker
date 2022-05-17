import Consumer.getClass

import java.io.{BufferedInputStream, BufferedOutputStream, BufferedReader, ByteArrayInputStream, ByteArrayOutputStream, FileNotFoundException, FileOutputStream, InputStreamReader, ObjectInputStream, ObjectOutputStream, PrintStream}
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
    var x = 0
    while(x<48){
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
      val message = MBUtils.SerializeObject(new Message(x.toString, priority, topic, value))
      os.println(message)

      if(x%2==0)
        Thread.sleep(1000)
    }

    val message = MBUtils.SerializeObject(new Connection("disconnect", Array[String]()))
    os.println(message)
    sock.close()
    println("Connection closed")
  }
}


class ProducerMessagesReceiveThread(is: BufferedReader) extends Thread
{
  override def run()
  {
    println("Messages receiving thread - started.")
    while(true){
      if(is.ready){
        val output = is.readLine
        val bytes = Base64.getDecoder.decode(output.getBytes(StandardCharsets.UTF_8))

        val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
        ois.readObject match {
          case confirmation: Confirmation =>
            println("Confirmed:" + confirmation.id)
          case _ => throw new Exception("Got not a message from client")
        }
        ois.close()
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

    val connectionMessage = MBUtils.SerializeObject(new Connection(clientType, Array[String]()))
    os.println(connectionMessage)

    val messagesReceiveThreadThread = new ProducerMessagesReceiveThread(is)
    messagesReceiveThreadThread.start()

    val messageGeneratingThread = new ProducerMessageGeneratingThread(sock, os)
    messageGeneratingThread.start()

  }
}
