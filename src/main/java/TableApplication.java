import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connectors.hive.HiveTableSink;
import org.apache.flink.connectors.hive.HiveTableSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.e;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * @Author Djh on  2021/5/27 10:38
 * @E-Mail 1544579459.djh@gmail.com
 */
public class TableApplication {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        changeLogTable(env);
//        hiveTest();
//        flinkUdfTest();
//        sinkHiveTest();
//        bundleWithDatasetAndStream();
//        hivePartitionTest();
//        sinkHiveTest();
//        tableApiTest();
        hiveTest();

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
        String hiveConfDir = "/Users/djh_mac/Desktop/FlinkStudy/src/main/resources/";

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


    private static void sinkHiveTest() {
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().inBatchMode().useBlinkPlanner().build();
        TableEnvironment tableEnvironment = TableEnvironment.create(environmentSettings);

        String name = "hive_test";
        String defaultDatabase = "default";
        String hiveConfDir = "/Users/djh_mac/Desktop/FlinkStudy/src/main/resources/";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tableEnvironment.registerCatalog("hive_test", hive);
        tableEnvironment.useCatalog("hive_test");
        tableEnvironment.getConfig().setSqlDialect(SqlDialect.HIVE);


        TableResult tableResult = tableEnvironment.executeSql("select name,sex,age from name_age_sex");
        CloseableIterator<Row> collect = tableResult.collect();

        ArrayList<Row> rows = new ArrayList<>();
        collect.forEachRemaining(new Consumer<Row>() {
            @Override
            public void accept(Row row) {
                if (((int) row.getField(2)) < 30) {
                    rows.add(row);
                }
            }
        });

        Table table = tableEnvironment.fromValues(DataTypes.ROW(
                DataTypes.FIELD("name", DataTypes.STRING()),
                DataTypes.FIELD("sex", DataTypes.STRING()),
                DataTypes.FIELD("age", DataTypes.INT())
        ), rows);

        Table noSexTable = table.dropColumns($("sex"));
        noSexTable.executeInsert("name_age");
    }

    private static void hivePartitionTest() {
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().inBatchMode().useBlinkPlanner().build();
        TableEnvironment tableEnvironment = TableEnvironment.create(environmentSettings);

        String name = "hive_test";
        String defaultDatabase = "default";
        String hiveConfDir = "/Users/djh_mac/Desktop/FlinkStudy/src/main/resources/";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tableEnvironment.registerCatalog("hive_test", hive);
        tableEnvironment.useCatalog("hive_test");
        tableEnvironment.getConfig().setSqlDialect(SqlDialect.HIVE);

        List<Row> rows = Arrays.asList(Row.of("djh", "20210628"), Row.of("hyt", "20210629"));
//        tableEnvironment.executeSql("set `hive.exec.dynamic.partition.mode=nonstrict`");
        Table table = tableEnvironment.fromValues(DataTypes.ROW(
                DataTypes.FIELD("name", DataTypes.STRING()),
                DataTypes.FIELD("dt", DataTypes.STRING())
        ), rows);
        table.executeInsert("partition_test_table");
    }

    private static void bundleWithDatasetAndStream() throws Exception {
        // with dataset
        System.out.println("-------------------data set-------------------");
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<Row> rowDataset = environment.fromElements(Row.of(1, "a"), Row.of(2, "b"));

        BatchTableEnvironment batchTableEnvironment = BatchTableEnvironment.create(environment);
        Table table = batchTableEnvironment.fromDataSet(rowDataset, $("num"), $("word"));
        table.execute().print();

        DataSet<Row> rowDataSet2 = batchTableEnvironment.toDataSet(table, Row.class);
        rowDataSet2.print();


        // with data stream
        System.out.println("-------------------data stream-------------------");
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> dataStream = streamExecutionEnvironment.fromElements(1, 2, 3, 4);

        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(streamExecutionEnvironment);
        Table numberTable = streamTableEnvironment.fromDataStream(dataStream, $("number"));
        numberTable.execute().print();

        DataStream<Tuple2<Boolean, Integer>> tuple2DataStream = streamTableEnvironment.toRetractStream(numberTable, Integer.class);
        DataStream<Integer> integerDataStream = streamTableEnvironment.toAppendStream(numberTable, Integer.class);
        tuple2DataStream.print();
        integerDataStream.print();
    }

    private static void tableApiTest() {
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().inBatchMode().useBlinkPlanner().build();
        TableEnvironment tableEnvironment = TableEnvironment.create(environmentSettings);

        String name = "hive_test";
        String defaultDatabase = "default";
        String hiveConfDir = "/Users/djh_mac/Desktop/FlinkStudy/src/main/resources/";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tableEnvironment.registerCatalog("hive_test", hive);
        tableEnvironment.useCatalog("hive_test");
        tableEnvironment.getConfig().setSqlDialect(SqlDialect.HIVE);

        tableEnvironment.from("tmp_region").select(
                $("region_id"),
                lit(123).as("Number value")
        ).where(lit(true).isTrue());
    }


}