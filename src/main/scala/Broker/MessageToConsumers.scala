package Broker

import SharedStructures.Message

import java.io.PrintStream
import java.util

case class MessageToConsumers(msg: Message, consumers: util.ArrayList[PrintStream])