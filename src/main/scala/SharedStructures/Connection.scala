package SharedStructures

class Connection(val clientId:String, val subject:String, val topics: Array[String]) extends Serializable