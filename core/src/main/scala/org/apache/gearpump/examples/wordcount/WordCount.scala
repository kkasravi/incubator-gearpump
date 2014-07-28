package org.apache.gearpump.app.examples.wordcount

/**
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

import akka.actor.Props
import org.apache.gearpump.{StageDescription, TaskDescription, AppDescription, HashPartitioner}
import org.apache.gearpump.client.ClientContext


class WordCount  {
}

object WordCount {

  def main(args: Array[String]) = {
    val context = ClientContext()

    args.toList match {
      case ip::port::rest =>
        val kvServiceURL = s"http://$ip:$port/kv"
        context.init(kvServiceURL)
        val appId = context.submit(getApplication())
        System.out.println(s"We get application id: $appId")


        val timeout = 60 * 10000; //60s
        Thread.sleep(timeout)
        context.shutdown(appId)
    }
  }

  def getApplication() : AppDescription = {
    val config = Map[String, Any]()
    val partitioner = new HashPartitioner()
    val split = TaskDescription(Props(classOf[Split]), partitioner)
    val sum = TaskDescription(Props(classOf[Sum]), partitioner)
    val app = AppDescription("wordCount", config, Array(StageDescription(split, 1), StageDescription(sum, 1)))
    app
  }
}
