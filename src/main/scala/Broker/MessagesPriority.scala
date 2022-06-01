package Broker

import SharedStructures.Message

import java.util

class MessagesPriority(val priority: Int, var msgs: util.LinkedList[Message])
