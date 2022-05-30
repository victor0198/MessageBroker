package Broker

import SharedStructures.Message

class MessageToSend(val msg: Message, var lastSentAt: Long)