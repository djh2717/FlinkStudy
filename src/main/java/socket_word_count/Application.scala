package socket_word_count


import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @author djh on  2021/4/22 21:12
 * @E-Mail 1544579459@qq.com
 */
object Application {
    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.createLocalEnvironment()
        env
            .socketTextStream("localhost", 9999)
            .flatMap((values, collect: Collector[(String, Int)]) => {
                values.split(" ").foreach(word => collect.collect((word, 1)))
            })
            .keyBy(_._1)
            .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
            .sum(1)
            .print()

        env.execute("Window word count")
    }
}
