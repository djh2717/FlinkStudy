import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.OutputTag;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
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
//        kafkaToHdfsTest(env);
//        iterateTest(env);
//        paramTest(env);
//        stateBackendTest(env);
        countWindowTest(env);

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

    private static void countWindowTest(StreamExecutionEnvironment streamExecutionEnvironment) {
        DataStreamSource<Integer> streamSource = streamExecutionEnvironment.addSource(new RichSourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> ctx) throws Exception {
                while (true) {
                    Thread.sleep(1000);
                    ctx.collect(1);
                }
            }

            @Override
            public void cancel() {

            }
        });

        streamSource
                .windowAll(GlobalWindows.create())
                .trigger(CountTrigger.of(3))
                .sum(0)
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

    private static void iterateTest(StreamExecutionEnvironment streamExecutionEnvironment) {
        streamExecutionEnvironment.setParallelism(1);

        DataStreamSource<Integer> source = streamExecutionEnvironment.fromElements(1, 2, 3, 4, 5, 12, 13, 14);

        IterativeStream<Integer> iterate = source.iterate(5000);

        OutputTag<Integer> lessThanTen = new OutputTag<Integer>("lessThanTen") {
        };

        SingleOutputStreamOperator<Integer> process = iterate.process(new ProcessFunction<Integer, Integer>() {
            @Override
            public void processElement(Integer value, Context ctx, Collector<Integer> out) throws Exception {
                if (value < 10) {
                    ctx.output(lessThanTen, value);
                } else {
                    value--;
                    out.collect(value);
                }
            }
        });

        iterate.closeWith(process);

        DataStream<Integer> result = process.getSideOutput(lessThanTen);
        result.print();
    }

    private static void paramTest(StreamExecutionEnvironment streamExecutionEnvironment) throws IOException {
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile("src/main/resources/flink-properties.properties");
        String value1 = parameterTool.get("value1");
        System.out.println(value1);


        streamExecutionEnvironment.registerCachedFile("src/main/resources/cache_file.txt", "cache_file");

        Configuration configuration = new Configuration();
        configuration.setString("par_test", "This is configuration test");
        ExecutionConfig executionConfig = streamExecutionEnvironment.getConfig();
        executionConfig.setGlobalJobParameters(configuration);

        String localVar = "This is local var!";

        DataStreamSource<Integer> intStream = streamExecutionEnvironment.fromElements(1, 23);

        intStream.map(new RichMapFunction<Integer, String>() {

            private String cache_file_value;
            private String paramTest;
            private String localParam;

            @Override
            public void open(Configuration parameters) throws Exception {
                File cache_file = getRuntimeContext().getDistributedCache().getFile("cache_file");
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(cache_file)));
                cache_file_value = bufferedReader.readLine();
                bufferedReader.close();

                // Get config param;
                Configuration globalJobParameters = (Configuration) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
                paramTest = globalJobParameters.getString(ConfigOptions.key("par_test").stringType().noDefaultValue());

                localParam = localVar;
            }

            @Override
            public String map(Integer value) throws Exception {
                return "value->" + value + "\t localParam->" + localParam + "\t paramConfiguration->" + paramTest + "\t   cacheFile->" + cache_file_value;
            }
        }).print();

    }

    private static void reduceAndAggregatingStateTest(StreamExecutionEnvironment streamExecutionEnvironment) {
        DataStreamSource<Tuple2<String, Integer>> data = streamExecutionEnvironment.fromElements(new Tuple2<String, Integer>("a", 1), new Tuple2<String, Integer>("a", 1), new Tuple2<String, Integer>("a", 1));

        data
                .keyBy(0)
                .map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    private ReducingState<Tuple2<String, Integer>> reducingState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ReducingStateDescriptor<Tuple2<String, Integer>> reduceStateDesc = new ReducingStateDescriptor<>("reduce_state", new ReduceFunction<Tuple2<String, Integer>>() {
                            @Override
                            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                                return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
                            }
                        }, Types.TUPLE(Types.STRING, Types.INT));
                        reducingState = getRuntimeContext().getReducingState(reduceStateDesc);
                    }

                    @Override
                    public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                        reducingState.add(value);

                        return reducingState.get();
                    }
                })
                .print();

        // Aggregating state
        data.keyBy(value -> value.f0)
                .map(new RichMapFunction<Tuple2<String, Integer>, Integer>() {

                    private AggregatingState<Tuple2<String, Integer>, Integer> aggregatingState;

                    @Override
                    public void open(Configuration parameters) throws Exception {

                    }

                    @Override
                    public Integer map(Tuple2<String, Integer> value) throws Exception {

                        return null;
                    }
                });
    }

    private static void checkpointConfigTest(StreamExecutionEnvironment streamExecutionEnvironment) {
        streamExecutionEnvironment.enableCheckpointing(5000);

        CheckpointConfig checkpointConfig = streamExecutionEnvironment.getCheckpointConfig();
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.setCheckpointTimeout(60000);
        // 两次检查点操作最小间隔时间.
        checkpointConfig.setMinPauseBetweenCheckpoints(10000);

        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 作业取消后检查点是否保存.
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        checkpointConfig.setTolerableCheckpointFailureNumber(3);


    }

    private static void stateBackendTest(StreamExecutionEnvironment streamExecutionEnvironment) {
        streamExecutionEnvironment.enableCheckpointing(5000);
        streamExecutionEnvironment.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        streamExecutionEnvironment.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        streamExecutionEnvironment.setStateBackend(new FsStateBackend("file:///Users/djh/IdeaProjects/FlinkStudy/src/main/resources/flink-checkpoints"));

        DataStreamSource<Integer> data = streamExecutionEnvironment.addSource(new RichSourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> ctx) throws Exception {
                while (true) {
                    int value = (int) (Math.random() * 10);
                    ctx.collect(value % 4);
                    Thread.sleep(2000);
                }
            }

            @Override
            public void cancel() {

            }
        });

        data.keyBy(value -> value)
                .map(new RichMapFunction<Integer, Integer>() {
                    private ValueState<Integer> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        state = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("int_state", Integer.class));
                    }

                    @Override
                    public Integer map(Integer value) throws Exception {
                        Integer value1 = state.value();
                        if (value1 != null) {
                            state.update(value1 + value);
                        } else {
                            state.update(value);
                        }

                        return state.value();
                    }
                }).printToErr();
    }

}