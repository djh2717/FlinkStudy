package utils;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * @Author Djh on  2021/7/3 18:06
 * @E-Mail 1544579459.djh@gmail.com
 */
public class DataProductUtil {
    public static DataSet<Tuple2<Integer, String>> getDatasetOfTuple2(ExecutionEnvironment environment) {
        return environment.fromElements(
                Tuple2.of(1, "a"),
                Tuple2.of(1, "a"),

                Tuple2.of(2, "b"),
                Tuple2.of(2, "b"),

                Tuple2.of(3, "c"),
                Tuple2.of(3, "c"),

                Tuple2.of(4, "d"),
                Tuple2.of(4, "d"),

                Tuple2.of(5, "e"),
                Tuple2.of(5, "e"),

                Tuple2.of(6, "f"),
                Tuple2.of(6, "f"),

                Tuple2.of(7, "g"),
                Tuple2.of(7, "g"),

                Tuple2.of(8, "h"),
                Tuple2.of(8, "h"),

                Tuple2.of(9, "i"),
                Tuple2.of(9, "i"),

                Tuple2.of(10, "j"),
                Tuple2.of(10, "j")
        );
    }

    public static DataStream<Tuple2<Integer, String>> getDataStreamOfTuple2(StreamExecutionEnvironment streamExecutionEnvironment) {
        return streamExecutionEnvironment.fromElements(
                Tuple2.of(1, "a"),
                Tuple2.of(1, "a"),

                Tuple2.of(2, "b"),
                Tuple2.of(2, "b"),

                Tuple2.of(3, "c"),
                Tuple2.of(3, "c"),

                Tuple2.of(4, "d"),
                Tuple2.of(4, "d"),

                Tuple2.of(5, "e"),
                Tuple2.of(5, "e"),

                Tuple2.of(6, "f"),
                Tuple2.of(6, "f"),

                Tuple2.of(7, "g"),
                Tuple2.of(7, "g"),

                Tuple2.of(8, "h"),
                Tuple2.of(8, "h"),

                Tuple2.of(9, "i"),
                Tuple2.of(9, "i"),

                Tuple2.of(10, "j"),
                Tuple2.of(10, "j")
        );
    }

    public static DataStream<Tuple2<Integer, String>> getUnlimitedDataStreamOne(StreamExecutionEnvironment environment) {
        return environment.addSource(new SourceFunction<Tuple2<Integer, String>>() {
            @Override
            public void run(SourceContext<Tuple2<Integer, String>> ctx) throws Exception {
                while (true) {
                    Thread.sleep(3000);

                    long now = System.currentTimeMillis();
                    ctx.collectWithTimestamp(Tuple2.of(1, "a"), now);
                    ctx.collectWithTimestamp(Tuple2.of(2, "b"), now);
                    ctx.collectWithTimestamp(Tuple2.of(3, "c"), now);
                    ctx.collectWithTimestamp(Tuple2.of(3, "c-2"), now);


                    ctx.collectWithTimestamp(Tuple2.of(5, "h"), now);
                }
            }

            @Override
            public void cancel() {

            }
        });
    }

    public static DataStream<Tuple2<Integer, String>> getUnlimitedDataStreamTwo(StreamExecutionEnvironment environment) {
        return environment.addSource(new SourceFunction<Tuple2<Integer, String>>() {
            @Override
            public void run(SourceContext<Tuple2<Integer, String>> ctx) throws Exception {
                while (true) {
                    Thread.sleep(3000);

                    long now = System.currentTimeMillis();
                    ctx.collectWithTimestamp(Tuple2.of(1, "d"), now);
                    ctx.collectWithTimestamp(Tuple2.of(2, "e"), now);
                    ctx.collectWithTimestamp(Tuple2.of(3, "f"), now);
                    ctx.collectWithTimestamp(Tuple2.of(3, "f-2"), now);


                    ctx.collectWithTimestamp(Tuple2.of(4, "g"), now);
                }
            }

            @Override
            public void cancel() {

            }
        });
    }


    public static DataStream<Tuple2<Integer, String>> getWaterMarkData(StreamExecutionEnvironment environment) {
        return environment.addSource(new SourceFunction<Tuple2<Integer, String>>() {
            @Override
            public void run(SourceContext<Tuple2<Integer, String>> ctx) throws Exception {
                while (true) {

                    long now = System.currentTimeMillis();
                    ctx.collectWithTimestamp(Tuple2.of(1, "a"), now + 1);
                    ctx.collectWithTimestamp(Tuple2.of(2, "b"), now + 100);
                    ctx.collectWithTimestamp(Tuple2.of(3, "c"), now + 30);
                    ctx.collectWithTimestamp(Tuple2.of(3, "c-2"), now + 10);

                    // 模拟迟到数据, 三秒之后,发送一条时间戳为三秒之前的数据.
                    Thread.sleep(3000);
                    ctx.collectWithTimestamp(Tuple2.of(4, "f-later-data"), now);

                    Thread.sleep(3000);
                }
            }

            @Override
            public void cancel() {

            }
        });
    }
}
