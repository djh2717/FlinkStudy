package water_mark_word_count

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._

import java.time.Duration


/**
 * @author djh on  2021/4/23 12:43
 * @E-Mail 1544579459@qq.com
 */
object Application {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.createLocalEnvironment()

        val waterStrategy = WatermarkStrategy
            .forBoundedOutOfOrderness[String](Duration.ofSeconds(10))
            .withTimestampAssigner(new SerializableTimestampAssigner[String] {
                override def extractTimestamp(element: String, recordTimestamp: Long): Long = {
                    recordTimestamp
                }
            })

        env
            .socketTextStream("localhost", 8888)
            .assignTimestampsAndWatermarks(waterStrategy)


    }

}