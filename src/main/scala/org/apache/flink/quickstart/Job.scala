package org.apache.flink.quickstart

import java.io.IOException
import java.net.URI
import javax.websocket.{ContainerProvider, OnClose, OnMessage, OnOpen, _}

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

//import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}

object Job {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val events = env.addSource(new ScalaMeetupStreamingSource())

    events.filter(_.venue != null)

    events.print()

    env.execute("Meetup Madrid DataStream")
  }
}

case class ScalaMeetupRSVGevent(
                                 var member: Member,
                                 var response: String,
                                 var visibility: String,
                                 var event: Event,
                                 var mtime: String,
                                 var guests: String,
                                 var rsvp_id: String,
                                 var group: Group,
                                 var venue: Venue
                               )

case class Member(
                   var member_name: String,
                   var photo: String,
                   var member_id: String
                 )

case class Event(
                  var time: String,
                  var event_url: String,
                  var event_id: String,
                  var event_name: String
                )

case class Group(
                  var group_name: String,
                  var group_city: String,
                  var group_lat: String,
                  var group_urlname: String,
                  var group_id: String,
                  var group_country: String,
                  var group_lon: String,
                  var group_topics: List[Group_topics]
                )

case class Group_topics(
                         var urlkey: String,
                         var topic_name: String
                       )

case class Venue(
                  var venue_id: String,
                  var venue_name: String,
                  var lon: String,
                  var lat: String
                )

@ClientEndpoint object ScalaMeetupEndpoint {

  trait MessageHandler {
    def handleMessage(message: String)
  }

}

@ClientEndpoint class ScalaMeetupEndpoint(val endpointURI: URI) {
  try {
    val container: WebSocketContainer = ContainerProvider.getWebSocketContainer
    container.connectToServer(this, endpointURI)
  } catch {
    case e: Exception => {
      throw new RuntimeException(e)
    }
  }
  var userSession: Session = null
  var messageHandler: ScalaMeetupEndpoint.MessageHandler = null
  val context: SourceFunction.SourceContext[ScalaMeetupRSVGevent] = null

  @OnOpen def onOpen(userSession: Session) {
    this.userSession = userSession
  }

  @OnClose def onClose(userSession: Session, reason: CloseReason) {
    this.userSession = null
  }

  @OnMessage def onMessage(message: String) {
    if (this.messageHandler != null) this.messageHandler.handleMessage(message)
  }

  def setMessageHandler(msgHandler: ScalaMeetupEndpoint.MessageHandler) {
    this.messageHandler = msgHandler
  }

  def sendMessage(message: String) {
    this.userSession.getAsyncRemote.sendText(message)
  }
}

class ScalaMeetupStreamingSource() extends RichSourceFunction[ScalaMeetupRSVGevent] {
  @throws[Exception]
  def run(sourceContext: SourceFunction.SourceContext[ScalaMeetupRSVGevent]) {
    val clientEndPoint = new ScalaMeetupEndpoint(new URI("wss://stream.meetup.com/2/rsvps"))
    clientEndPoint.setMessageHandler(new ScalaMeetupEndpoint.MessageHandler() {
      def handleMessage(message: String) {
        val mapper: ObjectMapper = new ObjectMapper
        mapper.registerModule(DefaultScalaModule)
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        var event: ScalaMeetupRSVGevent = null
        try {
          event = mapper.readValue(message, classOf[ScalaMeetupRSVGevent])
          sourceContext.collect(event)
        } catch {
          case e: IOException => {
            e.printStackTrace
          }
        }
      }
    })
    while (true) {
      Thread.sleep(2000)
    }
  }

  def cancel {
    System.out.println("cancel")
  }
}

