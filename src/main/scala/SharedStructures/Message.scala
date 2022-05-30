package SharedStructures

class Message(val id: String, val timeStamp: Long, val priority: Int, val topic: String, val value:Int) extends Serializable