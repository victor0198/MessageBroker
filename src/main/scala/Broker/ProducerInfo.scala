package Broker

import akka.actor.ActorRef

case class ProducerInfo(producerId:String, producerSender:ActorRef)
