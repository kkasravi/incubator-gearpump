/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.akkastream.example

import akka.NotUsed
import akka.actor.{Actor, ActorSystem, Props}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, ClosedShape}
import akka.stream.scaladsl._

import scala.concurrent.Future
 
/**
 * Stream example showing Broadcast
 */
object Test9 {
  // scalastyle:off println
  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer(
      ActorMaterializerSettings(system).withAutoFusing(false)
    )
    implicit val ec = system.dispatcher

    val echo = system.actorOf(Props(new Echo()))
    val sink = Sink.actorRef(echo, "COMPLETE")
    val flowA: Flow[Int, Int, NotUsed] =
      Flow[Int].map {
        x => println(s"processing broadcasted element : $x in flowA"); x
      }
    val flowB: Flow[Int, Int, NotUsed] = Flow[Int].map {
      x => println(s"processing broadcasted element : $x in flowB"); x
    }

    val graph = RunnableGraph.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._
      val bcast = b.add(Broadcast[Int](2))
      val source = Source(1 to 5)
      source ~> bcast.in
      bcast.out(0) ~> flowA ~> sink
      bcast.out(1) ~> flowB ~> sink
      ClosedShape
    })

    graph.run()
  }
  class Echo extends Actor {
    def receive: Receive = {
      case any: AnyRef =>
        println("Confirm received: " + any)
    }
  }
  // scalastyle:on println
}
