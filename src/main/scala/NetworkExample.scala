import java.io.{BufferedInputStream, BufferedOutputStream, PrintStream}
import java.net.ServerSocket

object NetworkExample {
  def main(args: Array[String]): Unit = {
    val ss = new ServerSocket(4444)
    val sock = ss.accept()
    val is = new BufferedInputStream(sock.getInputStream())
    val os = new PrintStream(new BufferedOutputStream(sock.getOutputStream()))
    os.println("Hi")
    os.flush()
    while(is.available() < 1){
      Thread.sleep(100)
    }
    val buf = new Array[Byte](is.available)
    is.read(buf)
    val input = new String(buf)
    println(input)
    os.println(input)
    os.flush()

  }

}
