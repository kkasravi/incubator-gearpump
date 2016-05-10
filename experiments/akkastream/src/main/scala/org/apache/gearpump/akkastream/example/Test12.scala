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
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, ClosedShape, UniformFanInShape}
import akka.stream.scaladsl._
import org.apache.gearpump.akkastream.GearpumpMaterializer

import scala.concurrent.{Await, Future}
 
/**
 * Partial source, sink examples
 */
object Test12 {
  // scalastyle:off println
  def main(args: Array[String]): Unit = {
    import scala.concurrent.duration._
    import akka.actor.ActorSystem
    import akka.stream.ActorMaterializer
    import akka.stream.scaladsl._

    implicit val system = ActorSystem()
    implicit val materializer = GearpumpMaterializer()
    implicit val ec = system.dispatcher

    val pickMaxOfThree = GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val zip1 = b.add(ZipWith[Int, Int, Int](math.max _))
      val zip2 = b.add(ZipWith[Int, Int, Int](math.max _))

      zip1.out ~> zip2.in0

      UniformFanInShape(zip2.out, zip1.in0, zip1.in1, zip2.in1)
    }

    val resultSink = Sink.head[Int]

    val g = RunnableGraph.fromGraph(GraphDSL.create(resultSink) { implicit b =>
      sink =>
        import GraphDSL.Implicits._

        // Importing the partial shape will return its shape (inlets & outlets)
        val pm3 = b.add(pickMaxOfThree)

        Source.single(1) ~> pm3.in(0)
        Source.single(2) ~> pm3.in(1)
        Source.single(3) ~> pm3.in(2)

        pm3.out ~> sink.in

        ClosedShape
    })

    val max: Future[Int] = g.run()
    max.map(x => println(s"maximum of three numbers : $x"))
    Await.result(max, 300.millis)
    // scalastyle:on println
  }
}
