package Broker

import akka.actor.{Actor, ActorRef}

case class CustomerSubscription(topics: Array[String], sender: ActorRef)
