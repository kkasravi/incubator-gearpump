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

import akka.actor.{Actor, ActorSystem, Props}
import akka.stream.ClosedShape
import org.apache.gearpump.akkastream.GearpumpMaterializer
import org.apache.gearpump.akkastream.graph.GraphPartitioner$
import akka.stream.scaladsl._

import scala.util.{Failure, Success}

/**
 * test fanout
 */
object Test5 {

  def main(args: Array[String]): Unit = {

    // scalastyle:off println
    println("running Test...")
    // scalastyle:on println

    implicit val system = ActorSystem("akka-test")
    implicit val materializer = GearpumpMaterializer()

    val echo = system.actorOf(Props(new Echo()))
    val sink = Sink.actorRef(echo, "COMPLETE")

    val source = Source(List(("male", "24"), ("female", "23")))

    RunnableGraph.fromGraph(
      GraphDSL.create() { implicit b =>
        import GraphDSL.Implicits._
        val unzip = b.add(Unzip[String, String]())

        val sink1 = Sink.actorRef(echo, "COMPLETE")
        val sink2 = Sink.actorRef(echo, "COMPLETE")

        source ~> unzip.in
        unzip.out0 ~> sink1
        unzip.out1 ~> sink1
        ClosedShape
      }
    ).run()
    system.shutdown()
    system.awaitTermination()
  }

  class Echo extends Actor {
    def receive: Receive = {
      case any: AnyRef =>
        // scalastyle:off println
        println("Confirm received: " + any)
        // scalastyle:on println
    }
  }
}
