package Broker

class BrokerAck(val producerId:String, val messageId:Int, val confirmationsNeeded:Int, var confirmationsReceived:Int)
