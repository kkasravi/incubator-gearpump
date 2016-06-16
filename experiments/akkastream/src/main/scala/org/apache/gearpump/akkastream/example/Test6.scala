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
import org.apache.gearpump.akkastream.GearpumpMaterializer
import org.apache.gearpump.akkastream.scaladsl.{Reduce, GroupBy, GearSource}
import akka.stream.scaladsl.Sink
import org.apache.gearpump.streaming.dsl.CollectionDataSource


/**
 *  WordCount example
 * Test GroupBy
 */

import org.apache.gearpump.akkastream.scaladsl.Implicits._

object Test6 {

  def main(args: Array[String]): Unit = {

    // scalastyle:off println
    println("running Test...")
    // scalastyle:on println

    implicit val system = ActorSystem("akka-test")
    implicit val materializer = GearpumpMaterializer()

    val echo = system.actorOf(Props(new Echo()))
    val sink = Sink.actorRef(echo, "COMPLETE")
    val sourceData = new CollectionDataSource(
      List("this is a good start", "this is a good time", "time to start", 
        "congratulations", "green plant", "blue sky"))
    val source = GearSource.from[String](sourceData)
    source.mapConcat{line =>
      line.split(" ").toList
    }.groupBy2(x => x).map(word => (word, 1))
      .reduce({(a, b) =>
        (a._1, a._2 + b._2)
      }).log("word-count").runWith(sink)

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
