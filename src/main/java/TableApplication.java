import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.types.Row;

import java.time.ZoneId;

/**
 * @Author Djh on  2021/5/27 10:38
 * @E-Mail 1544579459.djh@gmail.com
 */
public class TableApplication {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        changeLogTable(env);

        env.execute();
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

}