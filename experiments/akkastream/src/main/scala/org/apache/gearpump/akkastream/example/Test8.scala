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
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.stream.scaladsl._

import scala.concurrent.Future
 
/**
 * Stream example to find sum of elements
 */
object Test8 {

  def main(args: Array[String]): Unit = {
    // scalastyle:off println
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer(
      ActorMaterializerSettings(system).withAutoFusing(false)
    )
    implicit val ec = system.dispatcher

    // Source gives 1 to 100 elements
    val source: Source[Int, NotUsed] = Source(Stream.from(1).take(100)).async
    val sink: Sink[Int, Future[Int]] = Sink.fold[Int, Int](0)(_ + _).async

    val f: Future[Int] = source.runWith(sink)
    f.map(x => println(s"Sum of stream elements => $x"))
    // scalastyle:on println
    system.awaitTermination()
  }
}
