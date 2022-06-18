package SharedStructures

class Message(val id: Int, val producerId: String, val timeStamp: Long, val priority: Int, val topic: String, val value:Int) extends Serializable