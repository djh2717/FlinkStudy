CREATE TABLE if not exists dwd_dc_hu_parse_data
(
    `vin`         string,
    `sn`          string,
    `model`       string,
    `uuid`        string,
    `start_time`  bigint,
    `end_time`    bigint,
    `app_name`    string,
    `module_name` string,
    `page_name`   string,
    `event_name`  string,
    `data_value`  string,
    `all_code`    string
)
    PARTITIONED BY (`dt` string)
    stored as parquet;

-- 暂无  2.6.5在线收音机播放排行       2.6.3搜索排行

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


------------- 宽表  -------------
CREATE TABLE if not exists dws_buried_point_wide_table
(
    vin           string COMMENT "vin",
    time_value    timestamp COMMENT "事件时间",
    is_workday    string COMMENT "是否工作日",
    model         string COMMENT "车型",
    model_config  string COMMENT "版型",
    customer_type string COMMENT "客户类型",
    app_name      string COMMENT "应用",
    model_name    string COMMENT "模块",
    page_name     string COMMENT "页面",
    event_name    string COMMENT "事件",
    province      string COMMENT "省份",
    data_value    string COMMENT "值"
)
    partitioned by (dt string)
    stored as parquet;

with a as (select vin,
                  substr(from_unixtime(cast(substr(start_time, 0, 10) as int),
                                       "YYYY-MM-dd HH:mm:ss"), 0, 10) as time_value,
                  substr(from_unixtime(cast(substr(start_time, 0, 10) as int),
                                       "YYYY-MM-dd HH:mm:ss"), 0, 19) as time_value_with_hour,
                  model,
                  app_name,
                  module_name,
                  page_name,
                  event_name,
                  data_value,
                  dt
           from dwd_dc_hu_parse_data
           where dt between '$log7' and '$log3'),
     b as (select vin, if(customer_type = 0, "个人用户", "企业用户") as customer_type
           from dim_customer_type),
     c as (select substr(cur_date, 0, 10) as time_value, is_workday from dim_date),
     e as (select a.*, b.customer_type, c.is_workday, tc.config_code
           from b
                    join a on a.vin = b.vin
                    join c on a.time_value = c.time_value
                    join tmp_config tc on a.vin = tc.vin)
insert
overwrite
table
dws_buried_point_wide_table
partition
(
dt
)
select e.vin,
       e.time_value_with_hour,
       e.is_workday,
       e.model,
       e.config_code,
       e.customer_type,
       e.app_name,
       e.module_name,
       e.page_name,
       e.event_name,
       " ",
       e.data_value,
       e.dt
from e;


------------- 通用汇总  -------------
create table if not exists default.dws_buried_point_common_data
(
    is_work_day  string,
    year         string,
    month        string,
    time_value   string,
    car_type     string,
    edition_type string,
    application  string,
    module       string,
    page         string,
    event        string,
    report_times bigint
)
    partitioned by (dt string)
    stored as parquet;

set hive.exec.dynamic.partition.mode=nonstrict;
with a as (select is_workday,
                  substr(time_value, 1, 4)  as year,
                  substr(time_value, 6, 2)  as month,
                  substr(time_value, 12, 2) as time_value,
                  model,
                  " ",
                  app_name,
                  model_name,
                  page_name,
                  event_name,
                  count(*)                  as report_times,
                  dt
           from dws_buried_point_wide_table
           where dt between '$log7' and '$log3'
           group by is_workday, substr(time_value, 1, 4), substr(time_value, 6, 2),
                    substr(time_value, 12, 2), model, " ", app_name, model_name, page_name,
                    event_name, dt)
insert
overwrite
table
dws_buried_point_common_data
partition
(
dt
)
select is_workday,
       year,
       month,
       time_value,
       model,
       " ",
       app_name,
       model_name,
       page_name,
       event_name,
       report_times,
       dt
from a;


------------- 车机应用日活  -------------
create table if not exists dws_car_app_day_pv
(
    event_name                string,
    customer_type             string,
    open_app_times            int,
    open_on_workday_times     int,
    open_on_non_workday_times int
)
    partitioned by (dt string)
    stored as parquet;


with a as (select event_name, customer_type, count(*) as open_app_times, dt
           from dws_buried_point_wide_table
           where dt between '$log7' and '$log3'
             and app_name = "com.android.launcher3"
             and ((model = "EP30" and model_name = "home" and page_name = "Launcher")
               or (model = "EP21" and model_name = "launcher3" and page_name = "HomePageView"))
           group by event_name, customer_type, dt),

     b as (select event_name, customer_type, count(*) as open_app_times, dt
           from dws_buried_point_wide_table
           where dt between '$log7' and '$log3'
             and is_workday = "是"
             and app_name = "com.android.launcher3"
             and ((model = "EP30" and model_name = "home" and page_name = "Launcher")
               or (model = "EP21" and model_name = "launcher3" and page_name = "HomePageView"))
           group by event_name, customer_type, dt),

     c as (select event_name, customer_type, count(*) as open_app_times, dt
           from dws_buried_point_wide_table
           where dt between '$log7' and '$log3'
             and is_workday = "否"
             and app_name = "com.android.launcher3"
             and ((model = "EP30" and model_name = "home" and page_name = "Launcher")
               or (model = "EP21" and model_name = "launcher3" and page_name = "HomePageView"))
           group by event_name, customer_type, dt),

     d as (select a.event_name,
                  a.customer_type,
                  a.open_app_times as open_app_times,
                  b.open_app_times as open_on_workday_times,
                  c.open_app_times as open_on_non_workday_times,
                  a.dt
           from c
                    join b on c.event_name = b.event_name and
                              c.customer_type = b.customer_type
                    join a on c.event_name = a.event_name and
                              c.customer_type = a.customer_type)
insert
overwrite
table
dws_car_app_day_pv
partition
(
dt
)
select *
from d;


------------- 车机应用日活(时段)  -------------
create table if not exists dws_car_app_day_pv_time_period
(
    time_period               string,
    event_name                string,
    customer_type             string,
    open_app_times            int,
    open_on_workday_times     int,
    open_on_non_workday_times int
)
    partitioned by (dt string)
    stored as parquet;


with a as (select substr(time_value, 12, 2) as time_value,
                  event_name,
                  customer_type,
                  count(*)                  as open_app_times,
                  dt
           from dws_buried_point_wide_table
           where dt between '$log7' and '$log3'
             and app_name = "com.android.launcher3"
             and ((model = "EP30" and model_name = "home" and page_name = "Launcher")
               or (model = "EP21" and model_name = "launcher3" and page_name = "HomePageView"))
           group by substr(time_value, 12, 2), event_name, customer_type, dt),

     b as (select substr(time_value, 12, 2) as time_value,
                  event_name,
                  customer_type,
                  count(*)                  as open_app_times,
                  dt
           from dws_buried_point_wide_table
           where dt between '$log7' and '$log3'
             and is_workday = "是"
             and app_name = "com.android.launcher3"
             and ((model = "EP30" and model_name = "home" and page_name = "Launcher")
               or (model = "EP21" and model_name = "launcher3" and page_name = "HomePageView"))
           group by substr(time_value, 12, 2), event_name, customer_type, dt),

     c as (select substr(time_value, 12, 2) as time_value,
                  event_name,
                  customer_type,
                  count(*)                  as open_app_times,
                  dt
           from dws_buried_point_wide_table
           where dt between '$log7' and '$log3'
             and is_workday = "否"
             and app_name = "com.android.launcher3"
             and ((model = "EP30" and model_name = "home" and page_name = "Launcher")
               or (model = "EP21" and model_name = "launcher3" and page_name = "HomePageView"))
           group by substr(time_value, 12, 2), event_name, customer_type, dt),

     d as (select a.time_value,
                  a.event_name,
                  a.customer_type,
                  a.open_app_times as open_app_times,
                  b.open_app_times as open_on_workday_times,
                  c.open_app_times as open_on_non_workday_times,
                  a.dt
           from c
                    join b on c.time_value = b.time_value and c.event_name = b.event_name and
                              c.customer_type = b.customer_type
                    join a on a.time_value = c.time_value and a.event_name = c.event_name and
                              a.customer_type = c.customer_type)
insert
overwrite
table
dws_car_app_day_pv_time_period
partition
(
dt
)
select *
from d;


------------- 音乐本地与在线对比  -------------
create table if not exists dws_music_online_local
(
    year          string,
    month         string,
    model_name    string,
    customer_type string,
    times         int
)
    partitioned by (dt string)
    stored as parquet;

with a as (select substr(time_value, 1, 4) as year,
                  substr(time_value, 6, 2) as month,
                  model_name,
                  customer_type,
                  count(*)                 as times,
                  dt
           from dws_buried_point_wide_table
           where dt between '$log7' and '$log3'
             and model = "EP30"
             and app_name = "of.music.hz"
           group by substr(time_value, 1, 4), substr(time_value, 6, 2), model_name, customer_type,
                    dt)
insert
overwrite
table
dws_music_online_local
partition
(
dt
)
select *
from a;


------------- 音乐本地内部对比  -------------
create table if not exists dws_music_local_compare
(
    year          string,
    month         string,
    model_name    string,
    customer_type string,
    times         int
)
    partitioned by (dt string)
    stored as parquet;

with a as (select substr(time_value, 1, 4)                            as year,
                  substr(time_value, 6, 2)                            as month,
                  if(app_name = "of.music.hz", page_name, model_name) as name,
                  customer_type,
                  count(*)                                            as times,
                  dt
           from dws_buried_point_wide_table
           where dt between '$log7' and '$log3'
               and (model = "EP30" and app_name = "of.music.hz" and model_name = "本地音乐")
              or (model = "EP12" and app_name = "com.hazens.btmusic" or
                  app_name = "com.hazens.music")
           group by substr(time_value, 1, 4), substr(time_value, 6, 2),
                    if(app_name = "of.music.hz", page_name, model_name), customer_type, dt)
insert
overwrite
table
dws_music_local_compare
partition
(
dt
)
select *
from a;


------------- 音乐排行榜  -------------
create table if not exists dws_music_ranking
(
    year       string,
    month      string,
    model_name string,
    page_name  string,
    data_value string,
    times      int
)
    partitioned by (dt string)
    stored as parquet;

with a as (select substr(time_value, 1, 4) as year,
                  substr(time_value, 6, 2) as month,
                  model_name,
                  page_name,
                  data_value,
                  count(*)                 as times,
                  dt
           from dws_buried_point_wide_table
           where dt between '$log7' and '$log3'
             and model = "EP30"
             and app_name = "of.music.hz"
             and event_name = "play song"
           group by substr(time_value, 1, 4), substr(time_value, 6, 2), model_name, page_name,
                    data_value, dt)
insert
overwrite
table
dws_music_ranking
partition
(
dt
)
select *
from a;


------------- 总播放来源  -------------
create table if not exists dws_total_play_source
(
    year          string,
    month         string,
    page_name     string,
    customer_type string,
    times         int
)
    partitioned by (dt string)
    stored as parquet;

with a as (select substr(time_value, 1, 4) as year,
                  substr(time_value, 6, 2) as month,
                  page_name,
                  customer_type,
                  count(*)                 as times,
                  dt
           from dws_buried_point_wide_table
           where dt between '$log7' and '$log3'
             and model = "EP30"
             and app_name = "of.radio.hz"
             and model_name = "hzradio"
             and (event_name = "frequencyChange" or event_name = "playItem" or
                  event_name = "playSearchItemTypeId")
           group by substr(time_value, 1, 4), substr(time_value, 6, 2), page_name, customer_type,
                    dt)
insert
overwrite
table
dws_total_play_source
partition
(
dt
)
select *
from a;


------------- 主动点击来源  -------------
create table if not exists dws_self_click_source
(
    year          string,
    month         string,
    page_name     string,
    customer_type string,
    times         int
)
    partitioned by (dt string)
    stored as parquet;

with a as (select substr(time_value, 1, 4) as year,
                  substr(time_value, 6, 2) as month,
                  page_name,
                  customer_type,
                  count(*)                 as times,
                  dt
           from dws_buried_point_wide_table
           where dt between '$log7' and '$log3'
             and model = "EP30"
             and app_name = "of.radio.hz"
             and model_name = "hzradio"
             and (event_name = "frequencyChange" or event_name = "play")
           group by substr(time_value, 1, 4), substr(time_value, 6, 2), page_name, customer_type,
                    dt)
insert
overwrite
table
dws_self_click_source
partition
(
dt
)
select *
from a;


------------- 搜索排行  -------------
create table if not exists dws_search_ranking
(
    year       string,
    month      string,
    data_value string,
    times      int
)
    partitioned by (dt string)
    stored as parquet;

set hive.exec.dynamic.partition.mode=nonstrict;
with a as (select substr(time_value, 1, 4) as year,
                  substr(time_value, 6, 2) as month,
                  data_value,
                  count(*)                 as times,
                  dt
           from dws_buried_point_wide_table
           where dt between '$log7' and '$log3'
             and model = "EP30"
             and app_name = "of.radio.hz"
             and model_name = "hzradio"
             and page_name = "search"
             and event_name = "搜索关键词"
           group by substr(time_value, 1, 4), substr(time_value, 6, 2), data_value, dt)
insert
overwrite
table
dws_search_ranking
partition
(
dt
)
select *
from a;


------------- 本地收音机排行  -------------
create table if not exists dws_local_radio_ranking
(
    year       string,
    month      string,
    event_name string,
    date_value string,
    times      int
)
    partitioned by (dt string)
    stored as parquet;

with a as (select substr(time_value, 1, 4) as year,
                  substr(time_value, 6, 2) as month,
                  event_name,
                  data_value,
                  count(*)                 as times,
                  dt
           from dws_buried_point_wide_table
           where dt between '$log7' and '$log3'
             and model = "EP30"
             and app_name = "of.radio.hz"
             and model_name = "hzradio"
             and page_name like "%Local Radio%"
           group by substr(time_value, 1, 4), substr(time_value, 6, 2), event_name, data_value, dt)
insert
overwrite
table
dws_local_radio_ranking
partition
(
dt
)
select *
from a;


------------- 在线收音机排行  -------------
create table if not exists dws_online_radio_ranking
(
    year       string,
    month      string,
    event_name string,
    date_value string,
    times      int
)
    partitioned by (dt string)
    stored as parquet;

with a as (select substr(time_value, 1, 4) as year,
                  substr(time_value, 6, 2) as month,
                  event_name,
                  data_value,
                  count(*)                 as times,
                  dt
           from dws_buried_point_wide_table
           where dt between '$log7' and '$log3'
             and model = "EP30"
             and app_name = "of.radio.hz"
             and model_name = "hzradio"
             and (page_name = "考拉电台页面" or page_name = "考拉电台主页面（默认启动时的页面）")
           group by substr(time_value, 1, 4), substr(time_value, 6, 2), event_name, data_value, dt)
insert
overwrite
table
dws_online_radio_ranking
partition
(
dt
)
select *
from a;


------------- 语音埋点总体  -------------
create table if not exists dws_voice_buried_point_overall
(
    year         string,
    month        string,
    model        string,
    model_config string COMMENT "版型",
    event_name   string,
    date_value   string,
    namespace    string,
    name         string,
    dialog_id    string,
    id           string,
    times        int
)
    partitioned by (dt string)
    stored as parquet;

with a as (select substr(time_value, 1, 4)                          as year,
                  substr(time_value, 6, 2)                          as month,
                  model,
                  model_config,
                  event_name,
                  data_value,
                  get_json_object(data_value, "$.header.namespace") as namespace,
                  get_json_object(data_value, "$.header.name")      as name,
                  get_json_object(data_value, "$.header.dialog_id") as dialog_id,
                  get_json_object(data_value, "$.header.id")        as id,
                  count(*)                                          as times,
                  dt
           from dws_buried_point_wide_table
           where dt between '$log7' and '$log3'
             and app_name = "SpeechAssist"
             and model_name = "SpeechAssist"
             and page_name = "SpeechAssist"
           group by substr(time_value, 1, 4), substr(time_value, 6, 2), model, model_config,
                    event_name, data_value, get_json_object(data_value, "$.header.namespace"),
                    get_json_object(data_value, "$.header.name"),
                    get_json_object(data_value, "$.header.dialog_id"),
                    get_json_object(data_value, "$.header.id"),
                    dt)
insert
overwrite
table
dws_voice_buried_point_overall
partition
(
dt
)
select *
from a;


------------- 合成音播报  -------------
create table if not exists dws_synthetic_audio_announcement
(
    year         string,
    month        string,
    model        string,
    model_config string COMMENT "版型",
    namespace    string,
    name         string,
    dialog_id    string,
    id           string,
    times        int
)
    partitioned by (dt string)
    stored as parquet;

with a as (select substr(time_value, 1, 4)                          as year,
                  substr(time_value, 6, 2)                          as month,
                  model,
                  model_config,
                  get_json_object(data_value, "$.header.namespace") as namespace,
                  get_json_object(data_value, "$.header.name")      as name,
                  get_json_object(data_value, "$.header.dialog_id") as dialog_id,
                  get_json_object(data_value, "$.header.id")        as id,
                  dt
           from dws_buried_point_wide_table
           where dt between '$log7' and '$log3'
             and app_name = "SpeechAssist"
             and model_name = "SpeechAssist"
             and page_name = "SpeechAssist"
             and event_name = "语音服务类型"),
     b as (select substr(time_value, 1, 4)                     as year,
                  substr(time_value, 6, 2)                     as month,
                  model,
                  model_config,
                  get_json_object(data_value, "$.header.name") as name,
                  count(*)                                     as times,
                  dt
           from dws_buried_point_wide_table
           where dt between '$log7' and '$log3'
             and app_name = "SpeechAssist"
             and model_name = "SpeechAssist"
             and page_name = "SpeechAssist"
             and event_name = "语音服务类型"
           group by substr(time_value, 1, 4), substr(time_value, 6, 2), model, model_config,
                    get_json_object(data_value, "$.header.name"), dt),
     c as (select a.year,
                  a.month,
                  a.model,
                  a.model_config,
                  namespace,
                  a.name,
                  dialog_id,
                  id,
                  times,
                  a.dt
           from b
                    join a on a.year = b.year and a.month = b.month and a.model = b.model and
                              a.model_config = b.model_config and a.name = b.name and a.dt = b.dt)
insert
overwrite
table
dws_synthetic_audio_announcement
partition
(
dt
)
select *
from c;


------------- 语音命令  -------------
create table if not exists dws_voice_command
(
    year         string,
    month        string,
    model        string,
    model_config string COMMENT "版型",
    namespace    string,
    name         string,
    dialog_id    string,
    id           string,
    times        int
)
    partitioned by (dt string)
    stored as parquet;

with a as (select substr(time_value, 1, 4)                          as year,
                  substr(time_value, 6, 2)                          as month,
                  model,
                  model_config,
                  get_json_object(data_value, "$.header.namespace") as namespace,
                  get_json_object(data_value, "$.header.name")      as name,
                  get_json_object(data_value, "$.header.dialog_id") as dialog_id,
                  get_json_object(data_value, "$.header.id")        as id,
                  dt
           from dws_buried_point_wide_table
           where dt between '$log7' and '$log3'
             and app_name = "SpeechAssist"
             and model_name = "SpeechAssist"
             and page_name = "SpeechAssist"
             and event_name = "语音服务类型"
             and get_json_object(data_value, "$.header.namespace") != "SpeechSynthesizer"),
     b as (select substr(time_value, 1, 4)                          as year,
                  substr(time_value, 6, 2)                          as month,
                  model,
                  model_config,
                  get_json_object(data_value, "$.header.namespace") as namespace,
                  count(*)                                          as times,
                  dt
           from dws_buried_point_wide_table
           where dt between '$log7' and '$log3'
             and app_name = "SpeechAssist"
             and model_name = "SpeechAssist"
             and page_name = "SpeechAssist"
             and event_name = "语音服务类型"
             and get_json_object(data_value, "$.header.namespace") != "SpeechSynthesizer"
           group by substr(time_value, 1, 4), substr(time_value, 6, 2), model, model_config,
                    get_json_object(data_value, "$.header.namespace"), dt),
     c as (select a.year,
                  a.month,
                  a.model,
                  a.model_config,
                  a.namespace,
                  a.name,
                  dialog_id,
                  id,
                  times,
                  a.dt
           from b
                    join a on a.year = b.year and a.month = b.month and a.model = b.model and
                              a.model_config = b.model_config and a.namespace = b.namespace and
                              a.dt = b.dt)
insert
overwrite
table
dws_voice_command
partition
(
dt
)
select *
from c;


------------- 语音命令内容细分  -------------
create table if not exists dws_voice_command_detail
(
    year         string,
    month        string,
    model        string,
    model_config string COMMENT "版型",
    namespace    string,
    name         string,
    dialog_id    string,
    id           string,
    times        int
)
    partitioned by (dt string)
    stored as parquet;

with a as (select substr(time_value, 1, 4)                          as year,
                  substr(time_value, 6, 2)                          as month,
                  model,
                  model_config,
                  get_json_object(data_value, "$.header.namespace") as namespace,
                  get_json_object(data_value, "$.header.name")      as name,
                  get_json_object(data_value, "$.header.dialog_id") as dialog_id,
                  get_json_object(data_value, "$.header.id")        as id,
                  dt
           from dws_buried_point_wide_table
           where dt between '$log7' and '$log3'
             and app_name = "SpeechAssist"
             and model_name = "SpeechAssist"
             and page_name = "SpeechAssist"
             and event_name = "语音服务类型"
             and get_json_object(data_value, "$.header.namespace") != "SpeechSynthesizer"),
     b as (select substr(time_value, 1, 4)                          as year,
                  substr(time_value, 6, 2)                          as month,
                  model,
                  model_config,
                  get_json_object(data_value, "$.header.namespace") as namespace,
                  get_json_object(data_value, "$.header.name")      as name,
                  count(*)                                          as times,
                  dt
           from dws_buried_point_wide_table
           where dt between '$log7' and '$log3'
             and app_name = "SpeechAssist"
             and model_name = "SpeechAssist"
             and page_name = "SpeechAssist"
             and event_name = "语音服务类型"
             and get_json_object(data_value, "$.header.namespace") != "SpeechSynthesizer"
           group by substr(time_value, 1, 4), substr(time_value, 6, 2), model, model_config,
                    get_json_object(data_value, "$.header.namespace"),
                    get_json_object(data_value, "$.header.name"), dt),
     c as (select a.year,
                  a.month,
                  a.model,
                  a.model_config,
                  a.namespace,
                  a.name,
                  dialog_id,
                  id,
                  times,
                  a.dt
           from b
                    join a on a.year = b.year and a.month = b.month and a.model = b.model and
                              a.model_config = b.model_config and a.namespace = b.namespace and
                              a.name = b.name and a.dt = b.dt)
insert
overwrite
table
dws_voice_command_detail
partition
(
dt
)
select *
from c;


------------- 连续对话  -------------
create table if not exists dws_continue_dialogue
(
    rounds       int,
    rounds_times int
)
    partitioned by (dt string)
    stored as parquet;

with a as (select get_json_object(data_value, "$.header.dialog_id") as dialog_id,
                  count(*)                                          as rounds,
                  dt
           from dws_buried_point_wide_table
           where dt between '$log7' and '$log3'
             and app_name = "SpeechAssist"
             and model_name = "SpeechAssist"
             and page_name = "SpeechAssist"
           group by get_json_object(data_value, "$.header.dialog_id"), dt),
     b as (select rounds, dt, count(*) as rounds_times from a group by rounds, dt),
     c as (select a.rounds, b.rounds_times, a.dt
           from a
                    join b on a.rounds = b.rounds and a.dt = b.dt
           group by a.rounds, b.rounds_times, a.dt)
insert
overwrite
table
dws_continue_dialogue
partition
(
dt
)
select *
from c;


------------- 登录方式  -------------
create table if not exists dws_login_manner
(
    year              string,
    month             string,
    event_name        string,
    customer_type     string,
    EP30_report_times int,
    EP12_report_times int
)
    partitioned by (dt string)
    stored as parquet;

with a as (select substr(time_value, 1, 4) as year,
                  substr(time_value, 6, 2) as month,
                  event_name,
                  customer_type,
                  count(*)                 as ep30_report_times,
                  dt
           from dws_buried_point_wide_table
           where dt between '$log7' and '$log3'
             and app_name = "com.hozonauto.account"
             and model_name = "login_module"
             and page_name = "com.hozonauto.account.ui.activity.MobileCodeLoginActivity"
             and model = "EP30"
           group by substr(time_value, 1, 4), substr(time_value, 6, 2), event_name, customer_type,
                    dt),
     b as (select substr(time_value, 1, 4) as year,
                  substr(time_value, 6, 2) as month,
                  event_name,
                  customer_type,
                  count(*)                 as ep12_report_times,
                  dt
           from dws_buried_point_wide_table
           where dt between '$log7' and '$log3'
             and app_name = "com.hozonauto.account"
             and model_name = "login_module"
             and page_name = "com.hozonauto.account.ui.activity.MobileCodeLoginActivity"
             and model = "EP12"
           group by substr(time_value, 1, 4), substr(time_value, 6, 2), event_name, customer_type,
                    dt),
     c as (select a.year,
                  a.month,
                  a.event_name,
                  a.customer_type,
                  a.ep30_report_times,
                  b.ep12_report_times,
                  a.dt
           from a
                    full outer join b on a.year = b.year and a.month = b.month and
                                         a.event_name = b.event_name and
                                         a.customer_type = b.customer_type and
                                         a.dt = b.dt)
insert
overwrite
table
dws_login_manner
partition
(
dt
)
select *
from c;


------------- 升级方式  -------------
create table if not exists dws_update_manner
(
    year              string,
    month             string,
    event_name        string,
    customer_type     string,
    EP30_report_times int,
    EP12_report_times int
)
    partitioned by (dt string)
    stored as parquet;

with a as (select substr(time_value, 1, 4) as year,
                  substr(time_value, 6, 2) as month,
                  event_name,
                  customer_type,
                  count(*)                 as ep30_report_times,
                  dt
           from dws_buried_point_wide_table
           where dt between '$log7' and '$log3'
             and app_name = "com.carota.hozon"
             and model_name = "OTA"
             and page_name = "ModeSelectView"
             and model = "EP30"
           group by substr(time_value, 1, 4), substr(time_value, 6, 2), event_name, customer_type,
                    dt),
     b as (select substr(time_value, 1, 4) as year,
                  substr(time_value, 6, 2) as month,
                  event_name,
                  customer_type,
                  count(*)                 as ep12_report_times,
                  dt
           from dws_buried_point_wide_table
           where dt between '$log7' and '$log3'
             and app_name = "com.carota.hozon"
             and model_name = "OTA"
             and page_name = "ModeSelectView"
             and model = "EP12"
           group by substr(time_value, 1, 4), substr(time_value, 6, 2), event_name, customer_type,
                    dt),
     c as (select a.year,
                  a.month,
                  a.event_name,
                  a.customer_type,
                  a.ep30_report_times,
                  b.ep12_report_times,
                  a.dt
           from a
                    full outer join b on a.year = b.year and a.month = b.month and
                                         a.event_name = b.event_name and
                                         a.customer_type = b.customer_type and
                                         a.dt = b.dt)
insert
overwrite
table
dws_update_manner
partition
(
dt
)
select *
from c;


------------- 亮度调节  -------------
create table if not exists dws_dimming
(
    model      string,
    event_name string,
    value      string,
    times      int
)
    partitioned by (dt string)
    stored as parquet;

with a as (select model, event_name, data_value, count(*) as times, dt
           from dws_buried_point_wide_table
           where dt between '$log7' and '$log3'
               and (model = "EP30" and app_name = "com.ofilm.settings" and model_name = "bright" and
                    page_name = "LightFragment")
              or (model = "EP12" and app_name = "com.hz.car.setting" and
                  model_name = "HzSettings" and
                  page_name = "CommDisplayA" and event_name like "%亮度调节%")
           group by model, event_name, data_value, dt)
insert
overwrite
table
dws_dimming
partition
(
dt
)
select *
from a;


------------- 音量调节  -------------
create table if not exists dws_volume_adjust
(
    model      string,
    event_name string,
    value      string,
    times      int
)
    partitioned by (dt string)
    stored as parquet;

with a as (select model, event_name, data_value, count(*) as times, dt
           from dws_buried_point_wide_table
           where dt between '$log7' and '$log3'
               and (model = "EP30" and app_name = "com.ofilm.settings" and model_name = "volume" and
                    page_name = "SoundFragment")
              or (model = "EP12" and app_name = "com.hz.car.setting" and
                  model_name = "HzSettings" and
                  page_name = "CommDisplayA" and event_name like "%音量调节%")
           group by model, event_name, data_value, dt)
insert
overwrite
table
dws_volume_adjust
partition
(
dt
)
select *
from a;



