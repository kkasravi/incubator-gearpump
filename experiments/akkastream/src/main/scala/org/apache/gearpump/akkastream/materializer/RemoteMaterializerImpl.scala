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

package org.apache.gearpump.akkastream.materializer

import akka.actor.ActorSystem
import org.apache.gearpump.akkastream.GearAttributes
import org.apache.gearpump.akkastream.GearpumpMaterializer.Edge
import org.apache.gearpump.akkastream.module._
import org.apache.gearpump.akkastream.task.{BroadcastTask, GraphTask, SinkBridgeTask, SourceBridgeTask}
import akka.stream.impl.Stages.{Map => MMap}
import akka.stream.impl.StreamLayout.Module
import akka.stream.impl.Timers._
import akka.stream.impl.fusing.GraphStages.{MaterializedValueSource, SimpleLinearGraphStage, SingleSource}
import akka.stream.impl.fusing.{Map => FMap, _}
import akka.stream.impl.io.IncomingConnectionStage
import akka.stream.impl.{HeadOptionStage, Stages, Throttle}
import akka.stream.scaladsl._
import akka.stream.stage.AbstractStage.PushPullGraphStageWithMaterializedValue
import akka.stream.stage.GraphStage
import akka.stream.{FanInShape, FanOutShape}
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.dsl.StreamApp
import org.apache.gearpump.streaming.dsl.op._
import org.apache.gearpump.streaming.{ProcessorId, StreamApplication}
import org.apache.gearpump.util.Graph
import org.slf4j.LoggerFactory

import scala.concurrent.{Future, Promise}

/**
 * [[RemoteMaterializerImpl]] will materialize the [[Graph[Module, Edge]] to a Gearpump
 * Streaming Application.
 *
 * @param graph Graph
 * @param system ActorSystem
 */
class RemoteMaterializerImpl(graph: Graph[Module, Edge], system: ActorSystem) {

  import RemoteMaterializerImpl._

  type ID = String
  private implicit val actorSystem = system

  private def uuid: String = {
    java.util.UUID.randomUUID.toString
  }

  /**
   * @return a mapping from Module to Materialized Processor Id.
   */
  def materialize: (StreamApplication, Map[Module, ProcessorId]) = {
    val (opGraph, ids) = toOpGraph
    val app: StreamApplication = new StreamApp("app", system, UserConfig.empty, opGraph)
    val processorIds = resolveIds(app, ids)

    val updatedApp = updateJunctionConfig(processorIds, app)
    (removeIds(updatedApp), processorIds)
  }

  private def updateJunctionConfig(processorIds: Map[Module, ProcessorId],
      app: StreamApplication): StreamApplication = {
    val config = junctionConfig(processorIds)

    val dag = app.dag.mapVertex { vertex =>
      val processorId = vertex.id
      val newConf = vertex.taskConf.withConfig(config(processorId))
      vertex.copy(taskConf = newConf)
    }
    new StreamApplication(app.name, app.inputUserConfig, dag)
  }

  /**
   * Update junction config so that each GraphTask know its upstream and downstream.
   *
   * @param processorIds Map[Module,ProcessorId]
   * @return
   */
  private def junctionConfig(processorIds: Map[Module, ProcessorId]):
    Map[ProcessorId, UserConfig] = {
    val updatedConfigs = graph.vertices.map { vertex =>
      val processorId = processorIds(vertex)
      vertex.shape match {
        case shape: FanInShape[_] =>
          buildShape(vertex, processorIds)
        case shape: FanOutShape[_] =>
          buildShape(vertex, processorIds)
        case _ =>
          (processorId, UserConfig.empty)
      }
    }.toMap
    updatedConfigs
  }

  private def buildShape(vertex: Module, processorIds: Map[Module, ProcessorId]):
  (ProcessorId, UserConfig) = {
    val processorId = processorIds(vertex)
    val inProcessors = vertex.shape.inlets.map { inlet =>
      val upstreamModule = graph.incomingEdgesOf(vertex).find(tuple => {
        val (node1, edge, node2) = tuple
        val eq = edge.to.toString.equals(inlet.toString)
        eq
      }
      ).map(_._1)
      val upstreamProcessorId = processorIds(upstreamModule.get)
      upstreamProcessorId
    }.toList
    val outProcessors = vertex.shape.outlets.map { outlet =>
      val downstreamModule = graph.outgoingEdgesOf(vertex).find(tuple => {
        val (node1, edge, node2) = tuple
        val eq = edge.from.toString.equals(outlet.toString)
        eq

      }
      ).map(_._3)
      val downstreamProcessorId = downstreamModule.map(processorIds(_))
      downstreamProcessorId.get
    }.toList
    (processorId, UserConfig.empty.withValue(GraphTask.OUT_PROCESSORS, outProcessors).
      withValue(GraphTask.IN_PROCESSORS, inProcessors))
  }

  private def resolveIds(app: StreamApplication, ids: Map[Module, ID]):
  Map[Module, ProcessorId] = {
    ids.flatMap { kv =>
      val (module, id) = kv
      val processorId = app.dag.vertices.find { processor =>
        processor.taskConf.getString(id).isDefined
      }.map(_.id)
      processorId.map((module, _))
    }
  }

  private def removeIds(app: StreamApplication): StreamApplication = {
    val graph = app.dag.mapVertex{ processor =>
      val conf = removeId(processor.taskConf)
      processor.copy(taskConf = conf)
    }
    new StreamApplication(app.name, app.inputUserConfig, graph)
  }

  private def removeId(conf: UserConfig): UserConfig = {
    conf.filter{kv =>
      kv._2 != RemoteMaterializerImpl.TRACKABLE
    }
  }

  private def toOpGraph: (Graph[Op, OpEdge], Map[Module, ID]) = {
    var matValues = collection.mutable.Map.empty[Module, ID]
    val opGraph = graph.mapVertex[Op] { module =>
      val name = uuid
      val conf = UserConfig.empty.withString(name, RemoteMaterializerImpl.TRACKABLE)
      matValues += module -> name
      val parallelism = GearAttributes.count(module.attributes)
      val op = module match {
        case source: SourceTaskModule[_] =>
          val updatedConf = conf.withConfig(source.conf)
          new DataSourceOp(source.source, parallelism, updatedConf, "source")
        case sink: SinkTaskModule[_] =>
          val updatedConf = conf.withConfig(sink.conf)
          new DataSinkOp(sink.sink, parallelism, updatedConf, "sink")
        case sourceBridge: SourceBridgeModule[_, _] =>
          new ProcessorOp(classOf[SourceBridgeTask], parallelism = 1, conf, "source")
        case processor: ProcessorModule[_, _, _] =>
          val updatedConf = conf.withConfig(processor.conf)
          new ProcessorOp(processor.processor, parallelism, updatedConf, "source")
        case sinkBridge: SinkBridgeModule[_, _] =>
          new ProcessorOp(classOf[SinkBridgeTask], parallelism, conf, "sink")
        case groupBy: GroupByModule[t, g] =>
          new GroupByOp[t, g](groupBy.groupBy, parallelism, "groupBy", conf)
        case reduce: ReduceModule[_] =>
          reduceOp(reduce.f, conf)
        case graphStage: GraphStageModule =>
          translateGraphStageWithMaterializedValue(graphStage, parallelism, conf)
      }
      if (op == null) {
        throw new UnsupportedOperationException(
          module.getClass.toString + " is not supported with RemoteMaterializer"
        )
      }
      op
    }.mapEdge[OpEdge]{(n1, edge, n2) =>
      n2 match {
        case master: MasterOp =>
          Shuffle
        case slave: SlaveOp[_] if n1.isInstanceOf[ProcessorOp[_]] =>
          Shuffle
        case slave: SlaveOp[_] =>
          Direct
      }
    }
    (opGraph, matValues.toMap)
  }

  private def translateGraphStageWithMaterializedValue(module: GraphStageModule,
      parallelism: Int, conf: UserConfig): Op = {
    module.stage match {
      case graphStage: GraphStage[_] =>
        translateGraphStage(module, parallelism, conf)
      case headOptionStage: HeadOptionStage[_] =>
        headOptionOp(headOptionStage, conf)
      case pushPullGraphStageWithMaterializedValue:
        PushPullGraphStageWithMaterializedValue[_, _, _, _] =>
        translateSymbolic(pushPullGraphStageWithMaterializedValue, conf)

    }
  }

  private def translateGraphStage(module: GraphStageModule,
      parallelism: Int, conf: UserConfig): Op = {
    module.stage match {
      case balance: Balance[_] =>
        // TODO
        null
      case broadcast: Broadcast[_] =>
        new ProcessorOp(classOf[BroadcastTask], parallelism, conf, "broadcast")
      case collect: Collect[_, _] =>
        collectOp(collect.pf, conf)
      case concat: Concat[_] =>
        // TODO
        null
      case delayInitial: DelayInitial[_] =>
        // TODO
        null
      case dropWhile: DropWhile[_] =>
        dropWhileOp(dropWhile.p, conf)
      case flattenMerge: FlattenMerge[_, _] =>
        // TODO
        null
      case groupedWithin: GroupedWithin[_] =>
        // TODO
        null
      case idleInject: IdleInject[_, _] =>
        // TODO
        null
      case idleTimeoutBidi: IdleTimeoutBidi[_, _] =>
        // TODO
        null
      case incomingConnectionStage: IncomingConnectionStage =>
        // TODO
        null
      case interleave: Interleave[_] =>
        // TODO
        null
      case intersperse: Intersperse[_] =>
        // TODO
        null
      case limitWeighted: LimitWeighted[_] =>
        // TODO
        null
      case mapAsync: MapAsync[_, _] =>
        // TODO
        null
      case mapAsyncUnordered: MapAsyncUnordered[_, _] =>
        // TODO
        null
      case materializedValueSource: MaterializedValueSource[_] =>
        // TODO
        null
      case merge: Merge[_] =>
        MergeOp("mergePrefered", conf)
      case mergePreferred: MergePreferred[_] =>
        MergeOp("merge", conf)
      case mergeSorted: MergeSorted[_] =>
        // TODO
        null
      case prefixAndTail: PrefixAndTail[_] =>
        // TODO
        null
      case recover: Recover[_] =>
        // TODO
        null
      case scan: Scan[_, _] =>
        scanOp(scan.zero, scan.f, conf)
      case simpleLinearGraphStage: SimpleLinearGraphStage[_] =>
        translateSimpleLinearGraph(simpleLinearGraphStage, conf)
      case singleSource: SingleSource[_] =>
        singleSourceOp(singleSource.elem, conf)
      case split: Split[_] =>
        // TODO
        null
      case statefulMapConcat: StatefulMapConcat[_, _] =>
        statefulMapOp(statefulMapConcat.f, conf)
      case subSink: SubSink[_] =>
        // TODO
        null
      case subSource: SubSource[_] =>
        // TODO
        null
      case unfold: Unfold[_, _] =>
        // TODO
        null
      case unfoldAsync: UnfoldAsync[_, _] =>
        // TODO
        null
      case unzip: Unzip[_, _] =>
        // TODO
        null
      case zip: Zip[_, _] =>
        // TODO
        null
      case zipWith2: ZipWith2[_, _, _] =>
        zipWithOp(zipWith2.zipper, conf)
    }
  }

  private def translateSimpleLinearGraph(stage: SimpleLinearGraphStage[_], conf: UserConfig): Op = {
    stage match {
      case completion: Completion[_] =>
        // TODO
        null
      case delay: Delay[_] =>
        // TODO
        null
      case drop: Drop[_] =>
        dropOp(drop.count, conf)
      case dropWithin: DropWithin[_] =>
        // TODO
        null
      case filter: Filter[_] =>
        filterOp(filter.p, conf)
      case idle: Idle[_] =>
        // TODO
        null
      case initial: Initial[_] =>
        // TODO
        null
      case take: Take[_] =>
        takeOp(take.count, conf)
      case takeWhile: TakeWhile[_] =>
        // TODO
        null
      case takeWithin: TakeWithin[_] =>
        // TODO
        null
      case throttle: Throttle[_] =>
        // TODO
        null
    }
  }

  private def translateSymbolic(stage: PushPullGraphStageWithMaterializedValue[_, _, _, _],
      conf: UserConfig): Op = {
    stage match {
      case symbolicGraphStage: Stages.SymbolicGraphStage[_, _, _] =>
        symbolicGraphStage.symbolicStage match {
          case buffer: Stages.Buffer[_] =>
            // ignore the buffering operation
            identity("buffer", conf)
          case fold: Stages.Fold[_, _] =>
            foldOp(fold.zero, fold.f, conf)
          case grouped: Stages.Grouped[_] =>
            groupedOp(grouped.n, conf)
          case log: Stages.Log[Any] =>
            logOp(log.name, log.extract, conf)
          case map: Stages.Map[Any, Any] =>
            mapOp(map.f, conf)
          case sliding: Stages.Sliding[_] =>
            // TODO
            null
        }
    }
  }
}

object RemoteMaterializerImpl {
  final val NotApplied: Any => Any = _ => NotApplied

  def collectOp[In, Out](collect: PartialFunction[In, Out], conf: UserConfig): Op = {
    flatMapOp({ data: In =>
      collect.applyOrElse(data, NotApplied) match {
        case NotApplied => None
        case result: Any => Option(result)
      }
    }, "collect", conf)
  }

  def filterOp[In](filter: In => Boolean, conf: UserConfig): Op = {
    flatMapOp({ data: In =>
      if (filter(data)) Option(data) else None
    }, "filter", conf)
  }

  def headOptionOp[T](headOptionStage: HeadOptionStage[T], conf: UserConfig): Op = {
    val promise: Promise[Option[T]] = Promise()
    flatMapOp({ data: T =>
      data match {
        case None =>
          Some(promise.future.failed)
        case Some(d) =>
          promise.future.value
      }
    }, "headOption", conf)
  }

  def reduceOp[T](reduce: (T, T) => T, conf: UserConfig): Op = {
    var result: Option[T] = None
    val flatMap = { elem: T =>
      result match {
        case None =>
          result = Some(elem)
        case Some(r) =>
          result = Some(reduce(r, elem))
      }
      List(result)
    }
    flatMapOp(flatMap, "reduce", conf)
  }

  def zipWithOp[In1, In2, Out](zipWith: (In1, In2) => Out, conf: UserConfig): Op = {
    val flatMap = { elem: (In1, In2) =>
      val (e1, e2) = elem
      val result: Out = zipWith(e1, e2)
      List(Some(result))
    }
    flatMapOp(flatMap, "reduce", conf)
  }

  def identity(description: String, conf: UserConfig): Op = {
    flatMapOp({ data: Any =>
      List(data)
    }, description, conf)
  }

  def mapOp[T](map: T => T, conf: UserConfig): Op = {
    flatMapOp ({ data: T =>
      List(map(data))
    }, "map", conf)
  }

  def flatMapOp[In, Out](flatMap: In => Iterable[Out], conf: UserConfig): Op = {
    flatMapOp(flatMap, "flatmap", conf)
  }

  def flatMapOp[In, Out](fun: In => TraversableOnce[Out], description: String,
      conf: UserConfig): Op = {
    FlatMapOp(fun, description, conf)
  }

  def conflatOp[In, Out](seed: In => Out, aggregate: (Out, In) => Out,
    conf: UserConfig): Op = {
    var agg = None: Option[Out]
    val flatMap = {elem: In =>
      agg = agg match {
        case None =>
          Some(seed(elem))
        case Some(value) =>
          Some(aggregate(value, elem))
      }
      List(agg.get)
    }
    flatMapOp (flatMap, "map", conf)
  }

  def foldOp[In, Out](zero: Out, fold: (Out, In) => Out, conf: UserConfig): Op = {
    var aggregator: Out = zero
    val map = { elem: In =>
      aggregator = fold(aggregator, elem)
      List(aggregator)
    }
    flatMapOp(map, "fold", conf)
  }

  def groupedOp(count: Int, conf: UserConfig): Op = {
    var left = count
    val buf = {
      val b = Vector.newBuilder[Any]
      b.sizeHint(count)
      b
    }

    val flatMap: Any => Iterable[Any] = {input: Any =>
      buf += input
      left -= 1
      if (left == 0) {
        val emit = buf.result()
        buf.clear()
        left = count
        Some(emit)
      } else {
        None
      }
    }
    flatMapOp(flatMap, conf: UserConfig)
  }

  def dropOp(number: Long, conf: UserConfig): Op = {
    var left = number
    val flatMap: Any => Iterable[Any] = {input: Any =>
      if (left > 0) {
        left -= 1
        None
      } else {
        Some(input)
      }
    }
    flatMapOp(flatMap, "drop", conf)
  }

  def dropWhileOp[In](drop: In => Boolean, conf: UserConfig): Op = {
    flatMapOp({ data: In =>
      if (drop(data))  None else Option(data)
    }, "dropWhile", conf)
  }

  def logOp(name: String, extract: Any => Any, conf: UserConfig): Op = {
    val flatMap = {elem: Any =>
      LoggerFactory.getLogger(name).info(s"Element: {${extract(elem)}}")
      List(elem)
    }
    flatMapOp(flatMap, "log", conf)
  }

  def singleSourceOp[T](elem: T, conf: UserConfig): Op = {
    flatMapOp({ data: T =>
      Option(elem)
    }, "singleSourceOp", conf)
  }

  def scanOp[In, Out](zero: Out, f: (Out, In) => Out, conf: UserConfig): Op = {
    var aggregator = zero
    var pushedZero = false

    val flatMap = {elem: In =>
      aggregator = f(aggregator, elem)

      if (pushedZero) {
        pushedZero = true
        List(zero, aggregator)
      } else {
        List(aggregator)
      }
    }
    flatMapOp(flatMap, "scan", conf)
  }

  def takeOp(count: Long, conf: UserConfig): Op = {
    var left: Long = count

    val filter: Any => Iterable[Any] = {elem: Any =>
      left -= 1
      if (left > 0) Some(elem)
      else if (left == 0) Some(elem)
      else None
    }
    flatMapOp(filter, "take", conf)
  }

  def statefulMapOp[In, Out](f: () => In => Iterable[Out], conf: UserConfig): Op = {
    flatMapOp ({ data: In =>
      List(f()(data))
    }, "statefulMapOp", conf)
  }

  /**
   * We use this attribute to track module to Processor
   *
   */
  val TRACKABLE = "track how module is fused to processor"
}
