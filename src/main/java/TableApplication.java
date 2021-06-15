import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import java.time.ZoneId;

/**
 * @Author Djh on  2021/5/27 10:38
 * @E-Mail 1544579459.djh@gmail.com
 */
public class TableApplication {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        changeLogTable(env);
//        hiveTest();
        flinkUdfTest();

//        env.execute();
    }


    private static void table(ExecutionEnvironment environment, StreamExecutionEnvironment streamExecutionEnvironment) {
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(streamExecutionEnvironment, environmentSettings);
        streamTableEnvironment.executeSql("create table Test(a int)");

        EnvironmentSettings environmentSettings1 = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tableEnvironment = TableEnvironment.create(environmentSettings1);


    }

    private static void changeLogTable(StreamExecutionEnvironment env) {
        DataStreamSource<Row> data = env.fromElements(
                Row.of("djh", 110),
                Row.of("xjp", 120),
                Row.of("mzd", 130),
                Row.of("ylp", 150),
                Row.of("djh", 100)
        );

        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env);

        Table inputTable = streamTableEnvironment.fromDataStream(data).as("name", "score");

        streamTableEnvironment.createTemporaryView("inputTable", inputTable);
        Table resultTable = streamTableEnvironment.sqlQuery("select name,sum(score) from inputTable group by name");

//        streamTableEnvironment.getConfig().setLocalTimeZone(ZoneId.of("China"));
        DataStream<Tuple2<Boolean, Row>> resultDataStream = streamTableEnvironment.toRetractStream(resultTable, Row.class);


        resultDataStream.map(value -> value.f1)
                .print();
    }

    private static void hiveTest() {
        EnvironmentSettings build = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tableEnvironment = TableEnvironment.create(build);

        String name = "hive_test";
        String defaultDatabase = "default";
        String hiveConfDir = "/Users/djh/IdeaProjects/FlinkStudy/target/classes/";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tableEnvironment.registerCatalog("hive_test", hive);

        // set the HiveCatalog as the current catalog of the session
        tableEnvironment.useCatalog("hive_test");
        tableEnvironment.getConfig().setSqlDialect(SqlDialect.HIVE);

        tableEnvironment.executeSql("show databases").print();

        tableEnvironment.executeSql("select vin, idle_start_time, idle_end_time, last_report_time, last_receive_time, dt" +
                " from dwd_car_idle " +
                "group by vin, idle_start_time, idle_end_time, last_report_time, last_receive_time, dt ").print();
    }

    private static void flinkUdfTest() {
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().inBatchMode().useBlinkPlanner().build();
        TableEnvironment tableEnvironment = TableEnvironment.create(environmentSettings);

        tableEnvironment.createTemporaryFunction("MyFlinkUdf", MyFlinkUdf.class);

        tableEnvironment.executeSql("select MyFlinkUdf()").print();
    }

    public static class MyFlinkUdf extends ScalarFunction {
        public String eval() {
            return "Hello! This is flink udf!";
        }
    }

}