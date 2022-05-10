import java.io.{BufferedReader, InputStreamReader, PrintStream}
import java.net.{ServerSocket, Socket}
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.UUID

case class User(sock: Socket, is: BufferedReader, ps: PrintStream, name: String)

class AcceptConnectionsThread(var ss: ServerSocket, users: ConcurrentLinkedQueue[User]) extends Thread
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
      users.add(new User(sock, is, os, uuid))
    }
  }
}

class AcceptMessagesThread(users: ConcurrentLinkedQueue[User]) extends Thread
{
  override def run()
  {
    println("Messages accepting thread - started.")
    while(true){
      users.forEach((user) => {
        if(user.is.ready) {
          val input = user.is.readLine
          if(input.equals("0"))
            println("Closing message from "  + user.name.substring(0,4))
          else
            println("Message from "  + user.name.substring(0,4) + ":" + input)

          users.forEach((toUser) => {
            toUser.ps.println("From " + user.name.substring(0,4) + ":" + input)
          })

          if(input.equals("0")){
            user.sock.close()
            users.remove(user)
          }
        }
      })

      Thread.sleep(1000)
    }
  }
}

object Server {

  def main(args: Array[String]) {
    val users = new ConcurrentLinkedQueue[User]()

    val ss = new ServerSocket(4444)

    val connectionsThread = new AcceptConnectionsThread(ss, users)
    connectionsThread.start()

    val messagesThread = new AcceptMessagesThread(users)
    messagesThread.start()

    Thread.sleep(1000)

    while (true){
      print("Connected clients: ")
      if(users.size() > 0){
        users.forEach((user) => {
          print(user.name.substring(0,4))
        })
      }else{
        print("No one")
      }
      println()
      Thread.sleep(3000)
    }
  }

}
