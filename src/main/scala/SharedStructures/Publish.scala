package SharedStructures

class Publish (val message: Message, var dup: Boolean, val qos: Int) extends Serializable
