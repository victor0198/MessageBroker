import java.io.{BufferedInputStream, BufferedOutputStream, BufferedReader, InputStreamReader, PrintStream}
import java.net.{ServerSocket, Socket}
import java.util.concurrent.ConcurrentLinkedQueue
import scala.io.StdIn

class MessageGeneratingThread(sock: Socket, os: PrintStream) extends Thread
{
  override def run()
  {
    println("Messages generating thread - started.")
    var r = 7 // scala.util.Random
    while(r > 0){
      Thread.sleep(1000)

      r -= 1
      if(r == 0)
        println("Sending closing message")
      else
        println("Sending: " + r.toString)
      os.println(r.toString)
    }
    sock.close()
    println("Connection closed")
  }
}


class MessagesReceiveThread(is: BufferedReader) extends Thread
{
  override def run()
  {
    println("Messages receiving thread - started.")
    while(true){
      if(is.ready){
        val output = is.readLine
        println("Received: " + output)
      }

//      Thread.sleep(1000)
    }
  }
}

object Client {
  def main(args: Array[String]) {
    val host = "localhost"
    val port = 4444
    val sock = new Socket(host, port)
    val is = new BufferedReader(new InputStreamReader(sock.getInputStream))
    val os = new PrintStream(sock.getOutputStream)

    val messagesReceiveThreadThread = new MessagesReceiveThread(is)
    messagesReceiveThreadThread.start()

    val messageGeneratingThread = new MessageGeneratingThread(sock, os)
    messageGeneratingThread.start()

//    for (x <- 1 to 5)
//    {
//      var th = new MessageGeneratingThread()
//      th.setName(x.toString())
//      th.start()
//    }
//
//    val ss = new ServerSocket(4444)
//    val sock = ss.accept()
//    val is = new BufferedInputStream(sock.getInputStream)
//    val os = new PrintStream(new BufferedOutputStream(sock.getOutputStream()))
//    os.println("Hi")
//    os.flush()
//    while(is.available() < 1){
//      Thread.sleep(100)
//    }
//    val buf = new Array[Byte](is.available)
//    is.read(buf)
//    val input = new String(buf)
//    println(input)
//    os.println(input)
//    os.flush()

  }
}
