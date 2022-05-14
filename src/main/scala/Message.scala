import java.io.Serializable

class Message(val id:String, val priority: Int, val topic: String, val value:Int) extends Serializable