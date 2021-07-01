import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.connectors.hive.HiveTableSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * @Author Djh on  2021/5/24 16:26
 * @E-Mail 1544579459.djh@gmail.com
 */
public class BatchApplication {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

//        coGroupTest(env);
//        outerJoin(env);
//        zipTest(env);
        hdfsFileTest(env);

        env.execute();
    }

    private static void coGroupTest(ExecutionEnvironment env) throws Exception {
        Tuple2<String, Integer> t1 = new Tuple2<>("a", 1);
        Tuple2<String, Integer> t2 = new Tuple2<>("b", 1);

        DataSource<Tuple2<String, Integer>> dataSet1 = env.fromElements(t1, t2);
        DataSource<Tuple2<String, Integer>> dataSet2 = env.fromElements(t1, t2);

        dataSet1.coGroup(dataSet2)
                .where(0)
                .equalTo(0)
                .with(new CoGroupFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String>() {
                    @Override
                    public void coGroup(Iterable<Tuple2<String, Integer>> first, Iterable<Tuple2<String, Integer>> second, Collector<String> out) throws Exception {
                        for (Tuple2<String, Integer> tuple : first) {
                            out.collect(tuple.toString());
                        }

                        for (Tuple2<String, Integer> tuple : second) {
                            out.collect(tuple.toString());
                        }
                        out.collect("-----");
                    }
                })
                .print();
    }

    private static void outerJoin(ExecutionEnvironment env) throws Exception {
        Tuple2<String, Integer> t1 = new Tuple2<>("a", 1);
        Tuple2<String, Integer> t2 = new Tuple2<>("b", 1);

        DataSource<Tuple2<String, Integer>> dataSet1 = env.fromElements(t1, t2);
        DataSource<Tuple2<String, Integer>> dataSet2 = env.fromElements(t1);

        dataSet1.leftOuterJoin(dataSet2)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String>() {

                    @Override
                    public String join(Tuple2<String, Integer> first, Tuple2<String, Integer> second) throws Exception {
                        return first.toString() + "  " + second;
                    }
                })
                .print();
    }

    private static void zipTest(ExecutionEnvironment env) throws Exception {
        DataSource<String> dataSet = env.fromElements("a", "b", "c", "d");
        DataSet<Tuple2<Long, String>> tuple2DataSet = DataSetUtils.zipWithIndex(dataSet);
        tuple2DataSet.print();
    }

    private static void hdfsFileTest(ExecutionEnvironment environment) {
        environment.setParallelism(1);

        DataSource<String> stringDataSource = environment.readTextFile("hdfs://10.0.6.93:8020/user/hive/warehouse/dwd_car_idle/dt=20210528/000000_0");
        stringDataSource.flatMap((FlatMapFunction<String, String>) (value, out) -> {
            System.out.println(value);
            for (String s : value.split(",")) {
                out.collect(s);
            }
        }).returns(String.class)
                .writeAsText("hdfs://10.0.6.93:8020/tmp/flink.txt");

    }

    private static void joinTest(ExecutionEnvironment environment) {
        DataSource<Integer> firstData = environment.fromElements(1, 2, 3, 4);
        DataSource<Integer> second = environment.fromElements(2, 3, 4);

        firstData.joinWithTiny(second)
                .where(value -> value)
                .equalTo(value -> value);

    }

    private static void partitionAndGroupByTest() throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<Tuple2<Integer, String>> tuple2DataSource = executionEnvironment.fromElements(
                Tuple2.of(1, "a"),
                Tuple2.of(1, "a"),
                Tuple2.of(2, "b"),
                Tuple2.of(2, "b"),
                Tuple2.of(3, "c"),
                Tuple2.of(3, "c"),
                Tuple2.of(4, "d"),
                Tuple2.of(4, "d"),
                Tuple2.of(5, "d"),
                Tuple2.of(5, "d"),
                Tuple2.of(6, "d"),
                Tuple2.of(6, "d"),

                Tuple2.of(7, "d"),
                Tuple2.of(7, "d"),

                Tuple2.of(8, "d"),
                Tuple2.of(8, "d")

        );

//        Partitioning is a more low-level operation than groupBy and does not apply a function on the data. It rather defines how data is distributed across parallel task instances. Data can be partitioned with different methods such as hash partitioning or range partitioning.
//
//        groupBy is not an operation by itself. It always needs a function that is applied on the grouped DataSet such as reduce, groupReduce, or groupCombine. The groupBy API defines how records are grouped before they are given into the respective function. Grouping of records happens in two steps.
//
//                1. All records with the same grouping key must be moved to the same task instance. This is done by partitioning the data. Since there are usually more distinct grouping keys than task instances, a task instance must handle records with distinct grouping keys.
//                2. All records in the same task instance must be grouped on the key. This is usually done by sorting the data.
//        So, the first step of groupBy is partitioning.
        tuple2DataSource.groupBy(0)
                .reduceGroup(new GroupReduceFunction<Tuple2<Integer, String>, Object>() {
                    @Override
                    public void reduce(Iterable<Tuple2<Integer, String>> values, Collector<Object> out) throws Exception {
                        ArrayList<Tuple2> tuple2s = new ArrayList<>();
                        values.forEach(tuple2s::add);
                        System.out.println("-----------" + tuple2s.size() + "-----------");

                        for (Tuple2<Integer, String> value : tuple2s) {
                            System.out.println(Thread.currentThread().getId() + "    " + value.toString());
                        }
                    }
                })
                .print();

        tuple2DataSource.partitionByHash(0)
                .mapPartition(new MapPartitionFunction<Tuple2<Integer, String>, Object>() {
                    @Override
                    public void mapPartition(Iterable<Tuple2<Integer, String>> values, Collector<Object> out) throws Exception {
                        ArrayList<Tuple2> tuple2s = new ArrayList<>();
                        values.forEach(tuple2s::add);
                        System.out.println("-----------" + tuple2s.size() + "-----------");

                        for (Tuple2<Integer, String> value : tuple2s) {
                            System.out.println(Thread.currentThread().getId() + "    " + value.toString());
                        }
                    }
                })
                .print();

    }
}