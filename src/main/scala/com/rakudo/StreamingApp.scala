package com.rakudo

import scopt.OptionParser
import com.google.inject.{Guice, Inject, Singleton}
import com.rakudo.StreamingApp.Params
import com.typesafe.config.ConfigFactory
import com.rakudo.conf.KafkaConfiguration
import com.rakudo.module.MainModule
import com.rakudo.core.traits.Adapter
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}


object StreamingApp extends App {
  val parser = new OptionParser[Params]("StreamingApp") {
    head("Streaming App")

    opt[String]('i', "interval")
      .text("interval conf")
      .action((x, c) => c.copy(interval = x))

    opt[String]('s', "partition")
      .text("partition conf")
      .action((x, c) => c.copy(partition = x))

    help("help").text("prints this usage text")
  }

  parser.parse(args, Params()) match {
    case Some(params) =>
      println(params)
      val injector = Guice.createInjector(MainModule)
      val runner = injector.getInstance(classOf[StreamingApp])
      ConfigFactory.invalidateCaches()
      //runner.run(params)
    case _ => sys.exit(1)
  }

  val params = ParameterTool.fromArgs(args)
  val input: String = params.get("interval")
  val partition: String = params.get("partition")
  println(input, partition)

  case class Params(interval: String = "", partition: String = "")
}

@Singleton
class StreamingApp @Inject() (
                             source: DataStream[String],
                             kafkaConf: KafkaConfiguration,
                             adapter: Adapter
                             ) extends Serializable  {

  private def createNewStreamingContext: StreamExecutionEnvironment = {

    val adapterDs: DataStream[Map[String, String]] = adapter.extract(source)

    source.print()
    adapterDs.print()
    source.executionEnvironment
    adapterDs.executionEnvironment
  }

  def run(params: Params): Unit = {
    val env: StreamExecutionEnvironment = createNewStreamingContext
    env.execute("Flink Streaming App Template")
  }
}
