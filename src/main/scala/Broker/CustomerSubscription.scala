package Broker

import java.io.PrintStream
import akka.actor.{Actor, ActorRef}

case class CustomerSubscription(topics: Array[String], sender: ActorRef)
