import java.io.{BufferedReader, ByteArrayInputStream, FileNotFoundException, InputStreamReader, ObjectInputStream, PrintStream}
import java.net.{Socket}
import java.nio.charset.StandardCharsets
import java.util.{Base64, Properties}
import scala.io.Source

case class Consumer(sock: Socket, is: BufferedReader, ps: PrintStream, name: String, topic:String)

class ConsumerMessagesReceiveThread(is: BufferedReader, ps: PrintStream) extends Thread
{
  override def run()
  {
    println("Messages receiving thread - started.")
    var totalReceive = 3
    while(totalReceive > 0){
      if(is.ready){
        val input = is.readLine
        val bytes = Base64.getDecoder.decode(input.getBytes(StandardCharsets.UTF_8))

        val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
        val msgo = ois.readObject match {
          case msg: Message => msg
          case _ => throw new Exception("Got not a message from client")
        }
        ois.close()
        println("Received: " + msgo.topic + " " + msgo.value)
        totalReceive -= 1
      }

      if(totalReceive == 0){
        ps.println("saturated")
        println("Connection closed")
      }

      Thread.sleep(100)
    }
  }
}

object Consumer {
  def main(args: Array[String]) {
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

    ps.println(clientType)
    ps.println(valueType)

    val messagesReceiveThreadThread = new ConsumerMessagesReceiveThread(is, ps)
    messagesReceiveThreadThread.start()

  }
}
