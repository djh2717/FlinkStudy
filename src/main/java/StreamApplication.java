import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.OutputTag;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


/**
 * @Author Djh on  2021/5/20 10:36
 * @E-Mail 1544579459.djh@gmail.com
 */
public class StreamApplication {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

//        connect(env);

//        project(env);
//        windowTest(env);
//        counterTest(env);
//        broadcastStateTest(env);
//        joinTest(env);
//        waterMarkTest(env);
//        kafkaTest(env);

//        kafkaTest(env);
        kafkaToHdfsTest(env);

        env.execute();

    }

    private static void connect(StreamExecutionEnvironment env) {
        DataStreamSource<String> dataStream = env.fromElements("abcdaaaa", "efgh");
        DataStreamSource<Integer> dataStream2 = env.fromElements(1, 1, 2, 3, 4, 5, 6, 8, 7);

        dataStream
                .connect(dataStream2)
                .flatMap(new CoFlatMapFunction<String, Integer, String>() {
                    @Override
                    public void flatMap1(String value, Collector<String> out) throws Exception {
                        for (char c : value.toCharArray()) {
                            out.collect(String.valueOf(c));
                        }
                    }

                    @Override
                    public void flatMap2(Integer value, Collector<String> out) throws Exception {
                        out.collect(value.toString());
                    }
                })
                .print();
    }

    private static void project(StreamExecutionEnvironment environment) {
        DataStreamSource<Tuple3<String, String, Integer>> dataStream = environment.fromElements(new Tuple3<>("djh", "男", 22));

        dataStream.project(0, 2)
                .print();
    }

    private static void windowTest(StreamExecutionEnvironment streamExecutionEnvironment) {
        WatermarkStrategy<String> stringWatermarkStrategy = WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((SerializableTimestampAssigner<String>) (element, recordTimestamp) -> {
//                    System.out.println(System.currentTimeMillis());
                    return System.currentTimeMillis();
                });

        DataStreamSource<String> dataStream = streamExecutionEnvironment.socketTextStream("localhost", 9091);
        dataStream
                .assignTimestampsAndWatermarks(stringWatermarkStrategy)
                .map(value -> new Tuple2<>(value, 1)).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .sum(1)
                .print();
    }

    private static void counterTest(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<Integer> nums = env.fromElements(1, 2, 3, 4, 5);

        nums
                .map(new RichMapFunction<Integer, Integer>() {
                    private static final long serialVersionUID = 5007760875131437423L;
                    private IntCounter intCounter = new IntCounter(0);

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        getRuntimeContext().addAccumulator("intCounter_djh", intCounter);
                    }

                    @Override
                    public Integer map(Integer value) throws Exception {
                        intCounter.add(1);
                        return value;
                    }
                })
                .print();

        JobExecutionResult result = env.execute();
        System.out.println(result.getAccumulatorResult("intCounter_djh").toString());

    }

    private static void broadcastStateTest(StreamExecutionEnvironment env) {

        env.setParallelism(2);

        MapStateDescriptor<String, Integer> mapDesc = new MapStateDescriptor<>("b1", String.class, Integer.class);
        BroadcastStream<Integer> broadcastStream = env.addSource(new RichSourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> ctx) throws Exception {
                while (true) {
                    Thread.sleep(5000);
                    ctx.collect((int) (Math.random() * 1000));
                }
            }

            @Override
            public void cancel() {

            }
        }).broadcast(mapDesc);

        DataStreamSource<String> data = env.addSource(new RichSourceFunction<String>() {
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                String[] s = new String[]{"a", "b", "c", "d", "e", "e"};
                while (true) {
                    Thread.sleep(1000);
                    int index = (((int) (Math.random() * 10))) % s.length;
                    ctx.collect(s[index]);
                }
            }

            @Override
            public void cancel() {

            }
        });

        data
                .connect(broadcastStream)
                .process(new BroadcastProcessFunction<String, Integer, String>() {

                    private static final long serialVersionUID = -1104247896643956238L;
                    private String key;

                    @Override
                    public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        System.out.print(value + "-NoBroadCast          ");
                        for (Map.Entry<String, Integer> entry : ctx.getBroadcastState(mapDesc).immutableEntries()) {
                            System.out.print(entry.getKey() + "->" + entry.getValue() + "    ");
                        }
                        System.out.println(" ");
                    }

                    @Override
                    public void processBroadcastElement(Integer value, Context ctx, Collector<String> out) throws Exception {
                        key = "Broadcast:" + value;
                        ctx.getBroadcastState(mapDesc).put(key, value);
                    }
                })
                .printToErr();
    }

    private static void batchBroadcastVar() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> dataSet = env.fromElements("a", "b", "c");
        DataSet<Integer> broadcast = env.fromElements(1, 2, 3);

        dataSet
                .map(new RichMapFunction<String, String>() {
                    private static final long serialVersionUID = 488562145815121763L;
                    private List<Integer> list = new ArrayList<>();

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        List<Integer> nums = getRuntimeContext().getBroadcastVariable("nums");

                        list.addAll(nums);
                    }

                    @Override
                    public String map(String value) throws Exception {
                        return value + ": " + list.toString();
                    }
                })
                .withBroadcastSet(broadcast, "nums")
                .print();
    }

    private static void joinTest(StreamExecutionEnvironment env) {
        String[] s = new String[]{"a", "b", "c", "d", "e"};

        DataStreamSource<Tuple2<String, Integer>> stream1 = env.addSource(new RichSourceFunction<Tuple2<String, Integer>>() {
            @Override
            public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
                while (true) {
                    Thread.sleep(1000);
                    int index = ((int) (Math.random() * 10)) % s.length;
                    ctx.collect(new Tuple2<>(s[index], ((int) (Math.random() * 1000))));
                }
            }

            @Override
            public void cancel() {

            }
        });

        DataStreamSource<Tuple2<String, Integer>> stream2 = env.addSource(new RichSourceFunction<Tuple2<String, Integer>>() {
            @Override
            public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
                while (true) {
                    Thread.sleep(1000);
                    int index = ((int) (Math.random() * 10)) % s.length;
                    ctx.collect(new Tuple2<>(s[index], ((int) (Math.random() * 1000))));
                }
            }

            @Override
            public void cancel() {

            }
        });

        stream1.join(stream2)
                .where(value -> value.f0)
                .equalTo(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .apply((JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String>)
                        (first, second) -> "First:" + first.toString() + "  Second:" + second.toString())
                .print();

    }

    private static void waterMarkTest(StreamExecutionEnvironment env) {
        RichSourceFunction<Integer> source = new RichSourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> ctx) throws Exception {
                while (true) {
                    Thread.sleep(1000);
                    ctx.collectWithTimestamp(1, System.currentTimeMillis());
                }
            }

            @Override
            public void cancel() {

            }
        };
        DataStreamSource<Integer> stream = env.addSource(source);

        WatermarkStrategy<Integer> waterMark = WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3));
        stream
                .assignTimestampsAndWatermarks(waterMark)
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new AllWindowFunction<Integer, Integer, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<Integer> values, Collector<Integer> out) throws Exception {
                        System.out.println(window.getStart() + "->" + window.getEnd());
                        int sum = 0;
                        for (Integer value : values) {
                            sum += value;
                        }
                        out.collect(sum);
                    }
                })
                .print();

    }

    private static void kafkaTest(StreamExecutionEnvironment streamExecutionEnvironment) {
        // Source
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink-kafka-consumer");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("TestComposeTopic", new SimpleStringSchema(), properties);
        // default
        consumer.setStartFromGroupOffsets();

        DataStream<String> stream = streamExecutionEnvironment
                .addSource(consumer);

        // Transformation
        SingleOutputStreamOperator<String> result = stream
                .map(value -> new Tuple2<>(value, 1)).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0)
                .sum(1)
                .map(Tuple2::toString);

        // Sink
        Properties properties1 = new Properties();
        properties1.setProperty("bootstrap.servers", "localhost:9092");
        FlinkKafkaProducer<String> producer =
                new FlinkKafkaProducer<>(
                        "Flink-Kafka-producer-Topic",
                        new SimpleStringSchema(),
                        properties1,
                        null,
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE, 10);

        result.addSink(producer);
    }

    private static void kafkaToHdfsTest(StreamExecutionEnvironment streamExecutionEnvironment) {
        // 配置kafka.
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink-kafka-consumer");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("TestComposeTopic", new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();

        DataStreamSource<String> data = streamExecutionEnvironment.addSource(kafkaConsumer);
        streamExecutionEnvironment.enableCheckpointing(6000L);

        //旁路输出
        OutputTag<Tuple2<Character, Integer>> oddNumber = new OutputTag<Tuple2<Character, Integer>>("oddNumber") {
        };
        OutputTag<Tuple2<Character, Integer>> evenNumber = new OutputTag<Tuple2<Character, Integer>>("evenNumber") {
        };

        SingleOutputStreamOperator<Tuple2<Character, Integer>> process = data
                .map((MapFunction<String, Tuple2<Character, Integer>>) value -> {
                    char character = value.toCharArray()[0];

                    return new Tuple2<>(character, (int) character);
                }).returns(Types.TUPLE(Types.CHAR, Types.INT))
                .process(new ProcessFunction<Tuple2<Character, Integer>, Tuple2<Character, Integer>>() {
                    private static final long serialVersionUID = -4972666473462225503L;

                    @Override
                    public void processElement(Tuple2<Character, Integer> value, Context ctx, Collector<Tuple2<Character, Integer>> out) throws Exception {
                        if (value.f1 % 2 == 0) {
                            ctx.output(evenNumber, value);
                        } else {
                            ctx.output(oddNumber, value);
                        }
                    }
                });


        // 配置FileSink.
        StreamingFileSink<String> stringStreamingFileSink_even = StreamingFileSink
                .forRowFormat(new Path("hdfs://10.0.6.93:8020/flink-to-hdfs/even"), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(3))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(1))
                                .withMaxPartSize(1024 * 1024)
                                .build())
                .build();
        StreamingFileSink<String> stringStreamingFileSink_odd = StreamingFileSink
                .forRowFormat(new Path("hdfs://10.0.6.93:8020/flink-to-hdfs/odd"), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(3))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(1))
                                .withMaxPartSize(1024 * 1024)
                                .build())
                .build();

        // 获取旁路输出,并sink到hdfs.
        DataStream<Tuple2<Character, Integer>> oddOutputStream = process.getSideOutput(oddNumber);
        oddOutputStream
                .map(Tuple2::toString)
                .addSink(stringStreamingFileSink_odd);

        DataStream<Tuple2<Character, Integer>> evenOutputStream = process.getSideOutput(evenNumber);
        evenOutputStream
                .map(Tuple2::toString)
                .addSink(stringStreamingFileSink_even);
    }
}