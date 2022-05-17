import java.io.{BufferedReader, ByteArrayInputStream, ByteArrayOutputStream, FileNotFoundException, InputStreamReader, ObjectInputStream, ObjectOutputStream, PrintStream}
import java.net.Socket
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.{Base64, Properties, UUID}
import scala.io.Source

case class Consumer(sock: Socket, is: BufferedReader, ps: PrintStream, name: String, topics: Array[String])

class ConsumerMessagesReceiveThread(is: BufferedReader, ps: PrintStream, receivedMessages: ConcurrentLinkedQueue[Message], sendNow: AtomicBoolean) extends Thread
{
  override def run()
  {
    println("Messages receiving thread - started.")
    while(true){
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
          if(msg.id == msgo.id){
            exists = true
          }
        })
        if(!exists){
          println("Received             : " + msgo.topic + " " + msgo.value + "| priority " + msgo.priority)
          receivedMessages.add(msgo)

          println("Sending confirmation:" + msgo.id)
          val connectionMessage = MBUtils.SerializeObject(new Confirmation(msgo.id))
          ps.println(connectionMessage)

        }
      }

      if(!sendNow.get()){
        Thread.sleep(5000)
      }
    }
  }
}

object Consumer {
  def main(args: Array[String]) {
    val receivedMessages = new ConcurrentLinkedQueue[Message]()

    val host = "localhost"
    val port = 4444
    val sock = new Socket(host, port)
    val is = new BufferedReader(new InputStreamReader(sock.getInputStream))
    val ps = new PrintStream(sock.getOutputStream)
    var sendNow = new AtomicBoolean(true)

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

    val connectionMessage = MBUtils.SerializeObject(new Connection(clientType, valueType.split(",")))
    ps.println(connectionMessage)

    val messagesReceiveThreadThread = new ConsumerMessagesReceiveThread(is, ps, receivedMessages, sendNow)
    messagesReceiveThreadThread.start()

    while(true){
      Thread.sleep(5000)
      sendNow.set(!sendNow.get())
      println("Processing...")
    }

  }
}
