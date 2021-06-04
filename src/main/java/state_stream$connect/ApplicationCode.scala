package state_stream$connect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.co.{RichCoFlatMapFunction, RichCoMapFunction}
import org.apache.flink.util.Collector

import java.util

/**
 * @author djh on  2021/4/16 18:48
 * @E-Mail 1544579459@qq.com
 */
object ApplicationCode {
  def main(args: Array[String]): Unit = {
    val sEnv = StreamExecutionEnvironment.createLocalEnvironment()

    val people1 = new util.ArrayList[People]()
    people1.add(People("mzd", 56))
    people1.add(People("hjt", 39))



    val people2 = new util.ArrayList[People]()
    people2.add(People("djh", 23))
    people2.add(People("xjp", 24))
    people2.add(People("mzd", 56))
    people2.add(People("hjt", 39))

    val dataStream1 = sEnv.fromCollection(people1).keyBy((_:People).name)
    val dataStream2 = sEnv.fromCollection(people2).keyBy((_:People).name)



    dataStream1
      .connect(dataStream2)
      .flatMap(new RichCoFlatMapFunction[People,People,People] {
        var keyedState: ValueState[Boolean] = _

        override def open(parameters: Configuration): Unit = {
          keyedState = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("state", classOf[Boolean]))
        }

        override def flatMap1(value: People, out: Collector[People]): Unit = {
          keyedState.update(true)
        }

        override def flatMap2(value: People, out: Collector[People]): Unit = {
          if (keyedState.value()==null) {
            out.collect(value)
          }
        }
      })
      .print()

    sEnv.execute()
  }
}