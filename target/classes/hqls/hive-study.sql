show tables;

add jar hdfs://10.0.6.93:8020/tmp/udftest.jar;

create temporary function udtf_test as "hive_udf.UdtfTest";

select udtf_test(data_value)
from dwd_dc_hu_parse_data;

select *
from dwd_dc_hu_parse_data;



------------ Reduce配置 ------------
-- Reduce 个数配置
-- set mapred.reduce.tasks = 15;
-- 设置每个reduce读取1G
set hive.exec.reducers.bytes.per.reducer=1073741824;


------------ 小文件合并 ------------
-- 设置map端输出进行合并，默认为true
set hive.merge.mapfiles = true;
-- 设置reduce端输出进行合并，默认为false
set hive.merge.mapredfiles = true;
-- 当输出文件的平均大小小于该值时，启动一个独立的MapReduce任务进行文件merge
set hive.merge.smallfiles.avgsize=134217728;
-- 执行前,合并小文件,让Map处理合适的数据量,启动合适数量的Map.
set mapred.max.split.size=134217728;
set mapred.min.split.size.per.node=134217728;
set mapred.min.split.size.per.rack=134217728;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;


------------ 压缩配置 ------------
-- hive的查询结果输出是否进行压缩
set hive.exec.compress.output=true;
--  MapReduce Job的结果输出是否使用压缩
set mapreduce.output.fileoutputformat.compress=true;


------------ 其他配置 ------------
-- 动态分区为非严格模式.
set hive.exec.dynamic.partition.mode=nonstrict;
-- 打开任务并行执行，默认为false, 同一个sql允许最大并行度，默认为8
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=16;
// 本地模式,默认为false. 满足如下两个条件才开启本地模式.输入小于128M,Map个数小于10个.
set hive.exec.mode.local.auto=true;
set hive.exec.mode.local.auto.inputbytes.max=134217728;
set hive.exec.mode.local.auto.tasks.max=10;
-- JVM 重用
set mapreduce.job.ubertask.enable=true;
-- 开启向量批处理模式,CDH6默认为true.
set hive.vectorized.execution.enabled=true;
-- fetch设置为more,让更多的查询不走MR.
set hive.fetch.task.conversion=more;

-- 134217728 128M, 1073741824 1G


set hive.map.aggr;
set hive.groupby.mapaggr.checkinterval;
set hive.groupby.skewindata;
set hive.input.format;
set hive.exec.reducers.bytes.per.reducer;


create table if not exists name_age_sex
(
    name string,
    age  int,
    sex  string
)
    row format delimited
        fields terminated by ",";

create table if not exists name_age
(
    name string,
    age  int
)
    row format delimited
        fields terminated by ",";

load data inpath "hdfs://10.0.6.93:8020/tmp/name_age_sex.txt"
    into table name_age_sex;

create table if not exists partition_test_table
(
    name string
)
    partitioned by (dt string)
    row format delimited
        fields terminated by ",";

drop table partition_test_table;

insert into name_age
values ('djh', 23);
