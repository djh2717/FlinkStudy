import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.util.Collector;

/**
 * @Author Djh on  2021/5/24 16:26
 * @E-Mail 1544579459.djh@gmail.com
 */
public class BatchApplication {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

//        coGroupTest(env);
//        outerJoin(env);
        zipTest(env);

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

    private static void test(ExecutionEnvironment env) {

    }

}