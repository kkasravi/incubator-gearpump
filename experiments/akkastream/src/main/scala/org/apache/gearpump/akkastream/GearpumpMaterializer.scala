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

package org.apache.gearpump.akkastream

import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.{ActorContext, ActorRef, ActorRefFactory, ActorSystem, Cancellable, ExtendedActorSystem}
import akka.event.{Logging, LoggingAdapter}
import akka.stream.Attributes.Attribute
import akka.stream._
import akka.stream.impl.Stages.SymbolicGraphStage
import akka.stream.impl.StreamLayout._
import akka.stream.impl._
import akka.stream.impl.fusing.{GraphInterpreterShell, GraphStageModule}
import akka.stream.stage.GraphStage
import org.apache.gearpump.akkastream.GearpumpMaterializer.Edge
import org.apache.gearpump.akkastream.graph.GraphPartitioner.Strategy
import org.apache.gearpump.akkastream.graph.LocalGraph.LocalGraphMaterializer
import org.apache.gearpump.akkastream.graph.RemoteGraph.RemoteGraphMaterializer
import org.apache.gearpump.akkastream.graph._

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.FiniteDuration

object GearpumpMaterializer {

  final val Debug = true

  final case class Edge(from: OutPort, to: InPort)

  final case class MaterializedValueSourceAttribute(mat: MaterializedValueNode) extends Attribute

  implicit def boolToAtomic(bool: Boolean): AtomicBoolean = new AtomicBoolean(bool)

  def apply(strategy: Strategy)(implicit context: ActorRefFactory):
  ExtendedActorMaterializer = {
    val system = actorSystemOf(context)

    apply(ActorMaterializerSettings(
      system).withAutoFusing(false), strategy, useLocalCluster = false, "flow")(context)
  }

  def apply(materializerSettings: Option[ActorMaterializerSettings] = None,
      strategy: Strategy = GraphPartitioner.AllRemoteStrategy,
      useLocalCluster: Boolean = true,
      namePrefix: Option[String] = None)(implicit context: ActorRefFactory):
    ExtendedActorMaterializer = {
    val system = actorSystemOf(context)

    val settings = materializerSettings getOrElse
      ActorMaterializerSettings(system).withAutoFusing(false)
    apply(settings, strategy, useLocalCluster, namePrefix.getOrElse("flow"))(context)
  }

  def apply(materializerSettings: ActorMaterializerSettings,
      strategy: Strategy,
      useLocalCluster: Boolean,
      namePrefix: String)(implicit context: ActorRefFactory):
    ExtendedActorMaterializer = {
    val system = actorSystemOf(context)

    new GearpumpMaterializer(
      system,
      materializerSettings,
      context.actorOf(
        StreamSupervisor.props(materializerSettings, false).withDispatcher(
          materializerSettings.dispatcher), StreamSupervisor.nextName()))
  }


  private def actorSystemOf(context: ActorRefFactory): ActorSystem = {
    val system = context match {
      case s: ExtendedActorSystem => s
      case c: ActorContext => c.system
      case null => throw new IllegalArgumentException("ActorRefFactory context must be defined")
      case _ =>
        throw new IllegalArgumentException(
          s"""
             |  context must be a ActorSystem or ActorContext, got [${context.getClass.getName}]
          """.stripMargin
        )
    }
    system
  }

}

/**
 *
 * [[GearpumpMaterializer]] allows you to render akka-stream DSL as a Gearpump
 * streaming application. If some module cannot be rendered remotely in Gearpump
 * Cluster, then it will use local Actor materializer as fallback to materialize
 * the module locally.
 *
 * User can customize a [[org.apache.gearpump.akkastream.graph.GraphPartitioner.Strategy]]
 * to determine which module should be rendered
 * remotely, and which module should be rendered locally.
 *
 * @see [[org.apache.gearpump.akkastream.graph.GraphPartitioner]]
 *     to find out how we cut the runnableGraph to two parts,
 *      and materialize them separately.
 * @param system          ActorSystem
 * @param strategy        Strategy
 * @param useLocalCluster whether to use built-in in-process local cluster
 */
class GearpumpMaterializer(override val system: ActorSystem,
    override val settings: ActorMaterializerSettings,
    override val supervisor: ActorRef,
    strategy: Strategy = GraphPartitioner.AllRemoteStrategy,
    useLocalCluster: Boolean = true, namePrefix: Option[String] = None)
  extends ExtendedActorMaterializer {

  private val subMaterializers: Map[Class[_], SubGraphMaterializer] = Map(
    classOf[LocalGraph] -> new LocalGraphMaterializer(system),
    classOf[RemoteGraph] -> new RemoteGraphMaterializer(useLocalCluster, system)
  )

  override def logger: LoggingAdapter = Logging.getLogger(system, this)

  override def isShutdown: Boolean = system.isTerminated

  override def effectiveSettings(opAttr: Attributes): ActorMaterializerSettings = {
    import ActorAttributes._
    import Attributes._
    opAttr.attributeList.foldLeft(settings) { (s, attr) =>
      attr match {
        case InputBuffer(initial, max) => s.withInputBuffer(initial, max)
        case Dispatcher(dispatcher) => s.withDispatcher(dispatcher)
        case SupervisionStrategy(decider) => s.withSupervisionStrategy(decider)
        case _ => s
      }
    }
  }

  override def withNamePrefix(name: String): ExtendedActorMaterializer =
    throw new UnsupportedOperationException()

  override implicit def executionContext: ExecutionContextExecutor =
    throw new UnsupportedOperationException()

  override def schedulePeriodically(initialDelay: FiniteDuration,
      interval: FiniteDuration,
      task: Runnable): Cancellable =
    system.scheduler.schedule(initialDelay, interval, task)(executionContext)

  override def scheduleOnce(delay: FiniteDuration, task: Runnable): Cancellable =
    system.scheduler.scheduleOnce(delay, task)(executionContext)

  override def materialize[Mat](runnableGraph: Graph[ClosedShape, Mat]): Mat = {
    val initialAttributes = Attributes(
      Attributes.InputBuffer(
        settings.initialInputBufferSize,
        settings.maxInputBufferSize
      ) ::
      ActorAttributes.Dispatcher(settings.dispatcher) ::
      ActorAttributes.SupervisionStrategy(settings.supervisionDecider) ::
      Nil)

    val module = Fusing.aggressive(runnableGraph).module
    val info = module.info
    import _root_.org.apache.gearpump.util.{Graph => GGraph}
    val graph = GGraph.empty[Module, Edge]

    info.allModules.foreach(module => {
      if (module.isCopied) {
        val original = module.asInstanceOf[CopiedModule].copyOf
        graph.addVertex(original)
        module.shape.outlets.zip(original.shape.outlets).foreach(out => {
          val (cout, oout) = out
          val in = info.downstreams(cout)
          val downStreamModule = info.inOwners(in)
          if(downStreamModule.isCopied) {
            val downStreamOriginal = downStreamModule.asInstanceOf[CopiedModule].copyOf
            graph.addEdge(original, Edge(oout, in), downStreamOriginal)
          }
        })
      }
    })

    if(Debug) {
      val iterator = graph.topologicalOrderIterator
      while (iterator.hasNext) {
        val module = iterator.next()
        // scalastyle:off println
        module match {
          case graphStageModule: GraphStageModule =>
            graphStageModule.stage match {
              case symbolicGraphStage: SymbolicGraphStage[_, _, _] =>
                val symbolicName = symbolicGraphStage.symbolicStage.getClass.getSimpleName
                println(
                  s"${module.getClass.getSimpleName}(${symbolicName})"
                )
              case graphStage: GraphStage[_] =>
                val name = graphStage.getClass.getSimpleName
                println(
                  s"${module.getClass.getSimpleName}(${name})"
                )
              case other =>
                println(
                  s"${module.getClass.getSimpleName}(${other.getClass.getSimpleName})"
                )
            }
          case _ =>
            println(module.getClass.getSimpleName)
        }
        // scalastyle:on println
      }
    }

    val subGraphs = GraphPartitioner(strategy).partition(graph)
    val matValues = subGraphs.foldLeft(Map.empty[Module, Any]) { (map, subGraph) =>
      val materializer = subMaterializers(subGraph.getClass)
      map ++ materializer.materialize(subGraph, map)
    }
    val matNode = runnableGraph.module.materializedValueComputation
    // graph.topologicalOrderIterator.toVector.head.materializedValueComputation
    val mat = resolveMaterialized(matNode, matValues)
    mat.asInstanceOf[Mat]

  }

  override def materialize[Mat](runnableGraph: Graph[ClosedShape, Mat],
      subflowFuser: GraphInterpreterShell => ActorRef): Mat = {
    materialize(runnableGraph)
  }

  def shutdown: Unit = {
    subMaterializers.values.foreach(_.shutdown)
  }

  private def resolveMaterialized(mat: MaterializedValueNode,
      materializedValues: Map[Module, Any]): Any = mat match {
    case Atomic(m) => materializedValues.getOrElse(m, ())
    case Combine(f, d1, d2) => f(resolveMaterialized(d1, materializedValues),
      resolveMaterialized(d2, materializedValues))
    case Transform(f, d) => f(resolveMaterialized(d, materializedValues))
    case Ignore => ()
  }
}

