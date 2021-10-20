drop table if exists default.dwd_car_action;
create table if not exists default.dwd_car_action
(
    vin                           string comment "Vehicle ID",
    time_value                    timestamp comment "Reporting time",
    start_action                  int comment "Startup behavior      1 means yes, 0 means no, the same below",
    stop_action                   int comment "Stop behavior",
    quick_accelerate_action       int comment "Rapid acceleration",
    quick_down_action             int comment "Rapid deceleration",
    quick_turn_action             int comment "Sharp turning behavior",
    open_double_openLight_action  int comment "Double turn on the light behavior",
    close_double_openLight_action int comment "Turn off and double lights behavior",
    open_smallLight_action        int comment "Light-on behavior",
    close_smallLight_action       int comment "Turn off the light",
    open_dippedLight_action       int comment "Turn on low beam behavior",
    close_dippedLight_action      int comment "Turn off low beam behavior",
    open_highBeamLight_action     int comment "Turn on the high beam behavior",
    close_highBeamLight_action    int comment "Turn off the high beam behavior",
    open_leftTurnLight_action     int comment "Turn on the left turn signal",
    close_leftTurnLight_action    int comment "Turn off the left turn signal behavior",
    open_rightTurnLight_action    int comment "Turn on the right turn signal",
    close_rightTurnLight_action   int comment "Turn off the right turn signal behavior",
    open_breakLight_action        int comment "Turn on the brake light behavior",
    close_breakLight_action       int comment "Turn off the brake light behavior",
    open_moodLight_action         int comment "Atmosphere light on behavior",
    close_moodLight_action        int comment "Turn off the ambient light",
    open_dynamicLight_action      int comment "Turn on the activity indicator light behavior",
    close_dynamicLight_action     int comment "Off exercise ready indicator light behavior",
    open_driverDoor_action        int comment "Door-opening behavior",
    close_driverDoor_action       int comment "Door-closing behavior",
    open_coDriverDoor_action      int comment "Open the Co-driver door",
    close_coDriverDoor_action     int comment "The act of closing the passenger's door",
    open_leftAfterDoor_action     int comment "Behaviour of opening the left rear door",
    close_leftAfterDoor_action    int comment "Behaviour of closing the left rear door",
    open_rightAfterDoor_action    int comment "Behaviour of opening the right rear door",
    close_rightAfterDoor_action   int comment "Behaviour of closing the right rear door",
    open_endDoor_action           int comment "Tailgate behavior",
    close_endDoor_action          int comment "Tailgate behavior",
    open_skyWindow_action         int comment "Open Sunroof",
    close_skyWindow_action        int comment "Sunroof closed behavior",
    close_lock_action             int comment "Locking behavior",
    open_lock_action              int comment "Unlock behavior",
    open_airCondition_action      int comment "Air conditioning behavior",
    close_airCondition_action     int comment "Turn off air conditioning",
    close_driverBelt_action       int comment "Tying the driver's seat belt",
    open_driverBelt_action        int comment "apps.Solution of driving seat belt behavior",
    close_coDriverBelt_action     int comment "The act of tying the passenger seat belt",
    open_coDriverBelt_action      int comment "Solve the seat belt behavior of the co-driver",
    epe_eventtime                 timestamp,
    epe_eventcodes                string
)
    partitioned by (dt string)
    stored as parquet;


drop table default.dwd_car_action;


with allData as (select vin,
                        time_value,
                        g_status,
                        g_speed,
                        v_wheelangle,
                        v_doubleflashlightstatus,
                        v_smalllightstatus,
                        v_dippedheadlightstatus,
                        v_highbeamlightstatus,
                        v_leftturnlightstatus,
                        v_rightturnlightstatus,
                        v_brakebeamlightstatus,
                        v_moodlightstatus,
                        p_vcureadylightsts,
                        v_driversizedoorlockstatus,
                        v_codriversizedoorlockstatus,
                        v_leftafterdoorlockstatus,
                        v_rightafterdoorlockstatus,
                        v_carboarlockstatus,
                        v_skywindowstatus,
                        p_bmselectromagneticlocksts,
                        v_airstatus,
                        v_driversizebeltstatus,
                        v_codriversizebeltstatus,
                        row_number() over (order by vin,time_value) as row_number
                 from default.ods_analysis_data
                 where dt = "20210531"),
     a as (select * from allData),
     b as (select * from allData)
insert
overwrite
table
default.dwd_car_action
partition
(
dt = "20210531"
)
select c.vin_a,
       c.time_value_a,
       if(c.g_status_a = 2 and c.g_status_b = 1, 1, 0),
       if(c.g_status_a = 1 and c.g_status_b = 2, 1, 0),
       0,
       0,
       0,
       if(c.v_doubleflashlightstatus_a = 0 and c.v_doubleflashlightstatus_b = 1, 1, 0),
       if(c.v_doubleflashlightstatus_a = 1 and c.v_doubleflashlightstatus_b = 0, 1, 0),

       if(c.v_smalllightstatus_a = 0 and c.v_smalllightstatus_b = 1, 1, 0),
       if(c.v_smalllightstatus_a = 1 and c.v_smalllightstatus_b = 0, 1, 0),

       if(c.v_dippedheadlightstatus_a = 0 and c.v_dippedheadlightstatus_b = 1, 1, 0),
       if(c.v_dippedheadlightstatus_a = 1 and c.v_dippedheadlightstatus_b = 0, 1, 0),

       if(c.v_highbeamlightstatus_a = 0 and c.v_highbeamlightstatus_b = 1, 1, 0),
       if(c.v_highbeamlightstatus_a = 1 and c.v_highbeamlightstatus_b = 0, 1, 0),

       if(c.v_leftturnlightstatus_a = 0 and c.v_leftturnlightstatus_b = 1, 1, 0),
       if(c.v_leftturnlightstatus_a = 1 and c.v_leftturnlightstatus_b = 0, 1, 0),

       if(c.v_rightturnlightstatus_a = 0 and c.v_rightturnlightstatus_b = 1, 1, 0),
       if(c.v_rightturnlightstatus_a = 1 and c.v_rightturnlightstatus_b = 0, 1, 0),

       if(c.v_brakebeamlightstatus_a = 0 and c.v_brakebeamlightstatus_b = 1, 1, 0),
       if(c.v_brakebeamlightstatus_a = 1 and c.v_brakebeamlightstatus_b = 0, 1, 0),

       if(c.v_moodlightstatus_a = 0 and c.v_moodlightstatus_b = 1, 1, 0),
       if(c.v_moodlightstatus_a = 1 and c.v_moodlightstatus_b = 0, 1, 0),

       if(c.p_vcureadylightsts_a = 0 and c.p_vcureadylightsts_b = 1, 1, 0),
       if(c.p_vcureadylightsts_a = 1 and c.p_vcureadylightsts_b = 0, 1, 0),

       if(c.v_driversizedoorlockstatus_a = 0 and c.v_driversizedoorlockstatus_b = 1, 1, 0),
       if(c.v_driversizedoorlockstatus_a = 1 and c.v_driversizedoorlockstatus_b = 0, 1, 0),

       if(c.v_codriversizedoorlockstatus_a = 0 and c.v_codriversizedoorlockstatus_b = 1, 1, 0),
       if(c.v_codriversizedoorlockstatus_a = 1 and c.v_codriversizedoorlockstatus_b = 0, 1, 0),

       if(c.v_leftafterdoorlockstatus_a = 0 and c.v_leftafterdoorlockstatus_b = 1, 1, 0),
       if(c.v_leftafterdoorlockstatus_a = 1 and c.v_leftafterdoorlockstatus_b = 0, 1, 0),

       if(c.v_rightafterdoorlockstatus_a = 0 and c.v_rightafterdoorlockstatus_b = 1, 1, 0),
       if(c.v_rightafterdoorlockstatus_a = 1 and c.v_rightafterdoorlockstatus_b = 0, 1, 0),

       if(c.v_carboarlockstatus_a = 0 and c.v_carboarlockstatus_b = 1, 1, 0),
       if(c.v_carboarlockstatus_a = 1 and c.v_carboarlockstatus_b = 0, 1, 0),

       if(c.v_skywindowstatus_a = 0 and c.v_skywindowstatus_b = 1, 1, 0),
       if(c.v_skywindowstatus_a = 1 and c.v_skywindowstatus_b = 0, 1, 0),

       if(c.p_bmselectromagneticlocksts_a = 0 and c.p_bmselectromagneticlocksts_b = 1, 1, 0),
       if(c.p_bmselectromagneticlocksts_a = 1 and c.p_bmselectromagneticlocksts_b = 0, 1, 0),

       if(c.v_airstatus_a = 0 and c.v_airstatus_b = 1, 1, 0),
       if(c.v_airstatus_a = 1 and c.v_airstatus_b = 0, 1, 0),

       if(c.v_driversizebeltstatus_a = 0 and c.v_driversizebeltstatus_b = 1, 1, 0),
       if(c.v_driversizebeltstatus_a = 1 and c.v_driversizebeltstatus_b = 0, 1, 0),

       if(c.v_codriversizebeltstatus_a = 0 and c.v_codriversizebeltstatus_b = 1, 1, 0),
       if(c.v_codriversizebeltstatus_a = 1 and c.v_codriversizebeltstatus_b = 0, 1, 0)
from (
         select a.vin                          as vin_a,
                a.time_value                   as time_value_a,
                a.g_status                     as g_status_a,
                a.g_speed                      as g_speed_a,
                a.v_wheelangle                 as v_wheelangle_a,
                a.v_doubleflashlightstatus     as v_doubleflashlightstatus_a,
                a.v_smalllightstatus           as v_smalllightstatus_a,
                a.v_dippedheadlightstatus      as v_dippedheadlightstatus_a,
                a.v_highbeamlightstatus        as v_highbeamlightstatus_a,
                a.v_leftturnlightstatus        as v_leftturnlightstatus_a,
                a.v_rightturnlightstatus       as v_rightturnlightstatus_a,
                a.v_brakebeamlightstatus       as v_brakebeamlightstatus_a,
                a.v_moodlightstatus            as v_moodlightstatus_a,
                a.p_vcureadylightsts           as p_vcureadylightsts_a,
                a.v_driversizedoorlockstatus   as v_driversizedoorlockstatus_a,
                a.v_codriversizedoorlockstatus as v_codriversizedoorlockstatus_a,
                a.v_leftafterdoorlockstatus    as v_leftafterdoorlockstatus_a,
                a.v_rightafterdoorlockstatus   as v_rightafterdoorlockstatus_a,
                a.v_carboarlockstatus          as v_carboarlockstatus_a,
                a.v_skywindowstatus            as v_skywindowstatus_a,
                a.p_bmselectromagneticlocksts  as p_bmselectromagneticlocksts_a,
                a.v_airstatus                  as v_airstatus_a,
                a.v_driversizebeltstatus       as v_driversizebeltstatus_a,
                a.v_codriversizebeltstatus     as v_codriversizebeltstatus_a,

                b.g_status                     as g_status_b,
                b.g_speed                      as g_speed_b,
                b.v_wheelangle                 as v_wheelangle_b,
                b.v_doubleflashlightstatus     as v_doubleflashlightstatus_b,
                b.v_smalllightstatus           as v_smalllightstatus_b,
                b.v_dippedheadlightstatus      as v_dippedheadlightstatus_b,
                b.v_highbeamlightstatus        as v_highbeamlightstatus_b,
                b.v_leftturnlightstatus        as v_leftturnlightstatus_b,
                b.v_rightturnlightstatus       as v_rightturnlightstatus_b,
                b.v_brakebeamlightstatus       as v_brakebeamlightstatus_b,
                b.v_moodlightstatus            as v_moodlightstatus_b,
                b.p_vcureadylightsts           as p_vcureadylightsts_b,
                b.v_driversizedoorlockstatus   as v_driversizedoorlockstatus_b,
                b.v_codriversizedoorlockstatus as v_codriversizedoorlockstatus_b,
                b.v_leftafterdoorlockstatus    as v_leftafterdoorlockstatus_b,
                b.v_rightafterdoorlockstatus   as v_rightafterdoorlockstatus_b,
                b.v_carboarlockstatus          as v_carboarlockstatus_b,
                b.v_skywindowstatus            as v_skywindowstatus_b,
                b.p_bmselectromagneticlocksts  as p_bmselectromagneticlocksts_b,
                b.v_airstatus                  as v_airstatus_b,
                b.v_driversizebeltstatus       as v_driversizebeltstatus_b,
                b.v_codriversizebeltstatus     as v_codriversizebeltstatus_b
         from a
                  join b
                       on a.vin = b.vin and a.row_number + 1 = b.row_number
                           and (
                                  a.g_status != b.g_status or
                                  a.g_speed != b.g_speed or
                                  a.v_wheelangle != b.v_wheelangle or
                                  a.v_doubleflashlightstatus != b.v_doubleflashlightstatus or
                                  a.v_smalllightstatus != b.v_smalllightstatus or
                                  a.v_dippedheadlightstatus != b.v_dippedheadlightstatus or
                                  a.v_highbeamlightstatus != b.v_highbeamlightstatus or
                                  a.v_leftturnlightstatus != b.v_leftturnlightstatus or
                                  a.v_rightturnlightstatus != b.v_rightturnlightstatus or
                                  a.v_brakebeamlightstatus != b.v_brakebeamlightstatus or
                                  a.v_moodlightstatus != b.v_moodlightstatus or
                                  a.p_vcureadylightsts != b.p_vcureadylightsts or
                                  a.v_driversizedoorlockstatus != b.v_driversizedoorlockstatus or
                                  a.v_codriversizedoorlockstatus !=
                                  b.v_codriversizedoorlockstatus or
                                  a.v_leftafterdoorlockstatus != b.v_leftafterdoorlockstatus or
                                  a.v_rightafterdoorlockstatus != b.v_rightafterdoorlockstatus or
                                  a.v_carboarlockstatus != b.v_carboarlockstatus or
                                  a.v_skywindowstatus != b.v_skywindowstatus or
                                  a.p_bmselectromagneticlocksts != b.p_bmselectromagneticlocksts or
                                  a.v_airstatus != b.v_airstatus or
                                  a.v_driversizebeltstatus != b.v_driversizebeltstatus or
                                  a.v_codriversizebeltstatus != b.v_codriversizebeltstatus
                              )) c;

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
-- 优化后
with a as (select vin,
                  time_value,
                  g_status,
                  g_speed,
                  v_wheelangle,
                  v_doubleflashlightstatus,
                  v_smalllightstatus,
                  v_dippedheadlightstatus,
                  v_highbeamlightstatus,
                  v_leftturnlightstatus,
                  v_rightturnlightstatus,
                  v_brakebeamlightstatus,
                  v_moodlightstatus,
                  p_vcureadylightsts,
                  v_driversizedoorlockstatus,
                  v_codriversizedoorlockstatus,
                  v_leftafterdoorlockstatus,
                  v_rightafterdoorlockstatus,
                  v_carboarlockstatus,
                  v_skywindowstatus,
                  p_bmselectromagneticlocksts,
                  v_airstatus,
                  v_driversizebeltstatus,
                  v_codriversizebeltstatus,
                  row_number() over (order by vin,time_value) as row_number,
                  epe_eventcodes,
                  dt
           from default.ods_analysis_data
           where dt between "20210703" and "20210705"),
     b as (select * from a)
insert
overwrite
table
default.dwd_car_action
partition
(
dt
)
select *
from (select a.vin,
             a.time_value,
             if(a.g_status = 2 and b.g_status = 1, 1, 0)       as g_status_open_action,
             if(a.g_status = 1 and b.g_status = 2, 1, 0)       as g_status_close__action,
             0,
             0,
             0,
             if(a.v_doubleflashlightstatus = 0 and b.v_doubleflashlightstatus = 1, 1,
                0)                                             as v_doubleflashlightstatus_open_action,
             if(a.v_doubleflashlightstatus = 1 and b.v_doubleflashlightstatus = 0, 1,
                0)                                             as v_doubleflashlightstatus_close_ction,

             if(a.v_smalllightstatus = 0 and b.v_smalllightstatus = 1, 1,
                0)                                             as v_smalllightstatus_open_action,
             if(a.v_smalllightstatus = 1 and b.v_smalllightstatus = 0, 1,
                0)                                             as v_smalllightstatus_close_ction,

             if(a.v_dippedheadlightstatus = 0 and b.v_dippedheadlightstatus = 1, 1,
                0)                                             as v_dippedheadlightstatus_open_action,
             if(a.v_dippedheadlightstatus = 1 and b.v_dippedheadlightstatus = 0, 1,
                0)                                             as v_dippedheadlightstatus_close_ction,

             if(a.v_highbeamlightstatus = 0 and b.v_highbeamlightstatus = 1, 1,
                0)                                             as v_highbeamlightstatus_open_action,
             if(a.v_highbeamlightstatus = 1 and b.v_highbeamlightstatus = 0, 1,
                0)                                             as v_highbeamlightstatus_close_ction,

             if(a.v_leftturnlightstatus = 0 and b.v_leftturnlightstatus = 1, 1,
                0)                                             as v_leftturnlightstatus_open_action,
             if(a.v_leftturnlightstatus = 1 and b.v_leftturnlightstatus = 0, 1,
                0)                                             as v_leftturnlightstatus_close_ction,

             if(a.v_rightturnlightstatus = 0 and b.v_rightturnlightstatus = 1, 1,
                0)                                             as v_rightturnlightstatus_open_action,
             if(a.v_rightturnlightstatus = 1 and b.v_rightturnlightstatus = 0, 1,
                0)                                             as v_rightturnlightstatus_close_ction,

             if(a.v_brakebeamlightstatus = 0 and b.v_brakebeamlightstatus = 1, 1,
                0)                                             as v_brakebeamlightstatus_open_action,
             if(a.v_brakebeamlightstatus = 1 and b.v_brakebeamlightstatus = 0, 1,
                0)                                             as v_brakebeamlightstatus_close_ction,

             if(a.v_moodlightstatus = 0 and b.v_moodlightstatus = 1, 1,
                0)                                             as v_moodlightstatus_open_action,
             if(a.v_moodlightstatus = 1 and b.v_moodlightstatus = 0, 1,
                0)                                             as v_moodlightstatus_close_ction,

             if(a.p_vcureadylightsts = 0 and b.p_vcureadylightsts = 1, 1,
                0)                                             as p_vcureadylightsts_open_action,
             if(a.p_vcureadylightsts = 1 and b.p_vcureadylightsts = 0, 1,
                0)                                             as p_vcureadylightsts_close_ction,

             if(a.v_driversizedoorlockstatus = 0 and b.v_driversizedoorlockstatus = 1, 1,
                0)                                             as v_driversizedoorlockstatus_open_action,
             if(a.v_driversizedoorlockstatus = 1 and b.v_driversizedoorlockstatus = 0, 1,
                0)                                             as v_driversizedoorlockstatus_close_ction,

             if(a.v_codriversizedoorlockstatus = 0 and b.v_codriversizedoorlockstatus = 1, 1,
                0)                                             as v_codriversizedoorlockstatus_open_action,
             if(a.v_codriversizedoorlockstatus = 1 and b.v_codriversizedoorlockstatus = 0, 1,
                0)                                             as v_codriversizedoorlockstatus_close_ction,

             if(a.v_leftafterdoorlockstatus = 0 and b.v_leftafterdoorlockstatus = 1, 1,
                0)                                             as v_leftafterdoorlockstatus_open_action,
             if(a.v_leftafterdoorlockstatus = 1 and b.v_leftafterdoorlockstatus = 0, 1,
                0)                                             as v_leftafterdoorlockstatus_close_ction,

             if(a.v_rightafterdoorlockstatus = 0 and b.v_rightafterdoorlockstatus = 1, 1,
                0)                                             as v_rightafterdoorlockstatus_open_action,
             if(a.v_rightafterdoorlockstatus = 1 and b.v_rightafterdoorlockstatus = 0, 1,
                0)                                             as v_rightafterdoorlockstatus_close_ction,

             if(a.v_carboarlockstatus = 0 and b.v_carboarlockstatus = 1, 1,
                0)                                             as v_carboarlockstatus_open_action,
             if(a.v_carboarlockstatus = 1 and b.v_carboarlockstatus = 0, 1,
                0)                                             as v_carboarlockstatus_close_ction,

             if(a.v_skywindowstatus = 0 and b.v_skywindowstatus = 1, 1,
                0)                                             as v_skywindowstatus_open_action,
             if(a.v_skywindowstatus = 1 and b.v_skywindowstatus = 0, 1,
                0)                                             as v_skywindowstatus_close_ction,

             if(a.p_bmselectromagneticlocksts = 0 and b.p_bmselectromagneticlocksts = 1, 1,
                0)                                             as p_bmselectromagneticlocksts_open_action,
             if(a.p_bmselectromagneticlocksts = 1 and b.p_bmselectromagneticlocksts = 0, 1,
                0)                                             as p_bmselectromagneticlocksts_close_ction,

             if(a.v_airstatus = 0 and b.v_airstatus = 1, 1, 0) as v_airstatus_open_action,
             if(a.v_airstatus = 1 and b.v_airstatus = 0, 1, 0) as v_airstatus_close_ction,

             if(a.v_driversizebeltstatus = 0 and b.v_driversizebeltstatus = 1, 1,
                0)                                             as v_driversizebeltstatus_open_action,
             if(a.v_driversizebeltstatus = 1 and b.v_driversizebeltstatus = 0, 1,
                0)                                             as v_driversizebeltstatus_close_ction,

             if(a.v_codriversizebeltstatus = 0 and b.v_codriversizebeltstatus = 1, 1,
                0)                                             as v_codriversizebeltstatus_open_action,
             if(a.v_codriversizebeltstatus = 1 and b.v_codriversizebeltstatus = 0, 1,
                0)                                             as v_codriversizebeltstatus_close__actioclose_,
             a.time_value                                      as event_time,
             a.epe_eventcodes,
             a.dt
      from a
               join b
                    on a.vin = b.vin and a.row_number + 1 = b.row_number) c
where c.g_status_open_action != 0
   or c.g_status_close__action != 0

   or c.v_doubleflashlightstatus_close_ction != 0
   or c.v_doubleflashlightstatus_open_action != 0

   or c.v_smalllightstatus_close_ction != 0
   or c.v_smalllightstatus_open_action != 0

   or c.v_dippedheadlightstatus_close_ction != 0
   or c.v_dippedheadlightstatus_open_action != 0

   or c.v_highbeamlightstatus_close_ction != 0
   or c.v_highbeamlightstatus_open_action != 0

   or c.v_leftturnlightstatus_close_ction != 0
   or c.v_leftturnlightstatus_open_action != 0

   or c.v_rightturnlightstatus_close_ction != 0
   or c.v_rightturnlightstatus_open_action != 0

   or c.v_brakebeamlightstatus_close_ction != 0
   or c.v_brakebeamlightstatus_open_action != 0

   or c.v_moodlightstatus_open_action != 0
   or c.v_moodlightstatus_close_ction != 0

   or c.p_vcureadylightsts_open_action != 0
   or c.p_vcureadylightsts_close_ction != 0

   or c.v_driversizedoorlockstatus_close_ction != 0
   or c.v_driversizedoorlockstatus_open_action != 0

   or c.v_codriversizedoorlockstatus_close_ction != 0
   or c.v_codriversizedoorlockstatus_open_action != 0

   or c.v_leftafterdoorlockstatus_open_action != 0
   or c.v_leftafterdoorlockstatus_close_ction != 0

   or c.v_rightafterdoorlockstatus_close_ction != 0
   or c.v_rightafterdoorlockstatus_open_action != 0

   or c.v_carboarlockstatus_close_ction != 0
   or c.v_carboarlockstatus_open_action != 0

   or c.v_skywindowstatus_close_ction != 0
   or c.v_skywindowstatus_open_action != 0

   or c.p_bmselectromagneticlocksts_close_ction != 0
   or c.p_bmselectromagneticlocksts_open_action != 0

   or c.v_airstatus_close_ction != 0
   or c.v_airstatus_open_action != 0

   or c.v_driversizebeltstatus_open_action != 0
   or c.v_driversizebeltstatus_close_ction != 0

   or c.v_codriversizebeltstatus_close__actioclose_ != 0
   or c.v_codriversizebeltstatus_open_action != 0;


-- 验证
with a as (select vin,
                  time_value,
                  g_status,
                  g_speed,
                  v_wheelangle,
                  v_doubleflashlightstatus,
                  v_smalllightstatus,
                  v_dippedheadlightstatus,
                  v_highbeamlightstatus,
                  v_leftturnlightstatus,
                  v_rightturnlightstatus,
                  v_brakebeamlightstatus,
                  v_moodlightstatus,
                  p_vcureadylightsts,
                  v_driversizedoorlockstatus,
                  v_codriversizedoorlockstatus,
                  v_leftafterdoorlockstatus,
                  v_rightafterdoorlockstatus,
                  v_carboarlockstatus,
                  v_skywindowstatus,
                  p_bmselectromagneticlocksts,
                  v_airstatus,
                  v_driversizebeltstatus,
                  v_codriversizebeltstatus,
                  row_number() over (order by vin,time_value) as row_number
           from default.ods_analysis_data
           where dt = "20210531"),
     b as (select * from a)
select a.vin, b.vin, a.time_value, b.time_value, a.g_status, b.g_status
from a
         join b on a.row_number + 1 = b.row_number and a.vin = b.vin
limit 1000 offset 5000;


--------------------------- 报警表
drop table dwd_car_alarm;
create table if not exists default.dwd_car_alarm
(
    vin              string,
    time_value       timestamp,
    alarm_name       string,
    alarm_category   string,
    alarm_duration   string,
    alarm_start_time timestamp,
    alarm_stop_time  timestamp
)
    partitioned by (dt string)
    stored as parquet;

select a_alarmgeneralalarmflag,
       a_alarmmaxalarmlevel,
       a_alarmbatfaultcodes,
       a_alarmmotorfaultcodes,
       a_alarmenginefaultcodes,
       a_alarmotherfaultcodes,
       ep_faultcodes,
       ep_maxalarmlevel
from ods_analysis_data
where ep_faultcodes != ''
  and a_alarmbatfaultcodes != '';

select vin,
       a_alarmgeneralalarmflag,
       a_alarmmaxalarmlevel,
       time_value
--        first_value(time_value)
--                    over (partition by vin, a_alarmgeneralalarmflag,a_alarmmaxalarmlevel ),
--        last_value(time_value) over (partition by vin, a_alarmgeneralalarmflag,a_alarmmaxalarmlevel)
from ods_analysis_data
where a_alarmgeneralalarmflag != 0
  and a_alarmmaxalarmlevel != 0
order by vin,
         a_alarmgeneralalarmflag,
         a_alarmmaxalarmlevel,
         time_value;


--------------------------- 闲置表
drop table dwd_car_idle;

create table if not exists default.dwd_car_idle
(
    vin               string,
    idle_start_time   timestamp,
    idle_end_time     timestamp,
    last_report_time  timestamp,
    last_receive_time string
)
    partitioned by (dt string)
    stored as parquet;

set hive.exec.dynamic.partition.mode=nonstrict;
with a as (select vin, time_value, g_status, a_rectime, dt
           from ods_analysis_data),
     b as (select vin, dt, max(time_value) as idle_start_time
           from a
           where g_status = 1
           group by vin, dt),
     c as (select a.vin, min(time_value) as idle_end_time
           from b
                    left join a on a.vin = b.vin and a.g_status = 1
           where a.time_value > b.idle_start_time
           group by a.vin),
     d as (select b.dt, b.vin, b.idle_start_time, c.idle_end_time
           from c
                    left join b on b.vin = c.vin
           where b.idle_start_time < c.idle_end_time),
     e as (select d.dt,
                  d.vin,
                  d.idle_start_time,
                  d.idle_end_time,
                  max(a.time_value) as last_report_time,
                  max(a.a_rectime)  as last_receive_time
           from d
                    left join a on a.vin = d.vin and a.dt = d.dt
           group by d.dt, d.vin, d.idle_start_time, d.idle_end_time)
insert
overwrite
table
dwd_car_idle
partition
(
dt
)
select e.vin,
       e.idle_start_time,
       e.idle_end_time,
       e.last_report_time,
       e.last_receive_time,
       e.dt
from e;


--- 验证
with a as (select vin, time_value, g_status, a_rectime, dt
           from ods_analysis_data),
     b as (select vin, dt, max(time_value) as idle_start_time
           from a
           where g_status = 1
           group by vin, dt),
     c as (select a.vin, min(time_value) as idle_end_time
           from a
                    join b on a.vin = b.vin and a.g_status = 1
           where a.time_value > b.idle_start_time
           group by a.vin),
     d as (select b.dt, b.vin, b.idle_start_time, c.idle_end_time
           from b
                    join c on b.vin = c.vin
           where b.idle_start_time < c.idle_end_time),
     e as (select d.dt,
                  d.vin,
                  d.idle_start_time,
                  d.idle_end_time,
                  max(a.time_value) as last_report_time,
                  max(a.a_rectime)  as last_receive_time
           from a
                    join d on a.vin = d.vin and a.dt = d.dt
           group by d.dt, d.vin, d.idle_start_time, d.idle_end_time)
select *
from e;


with a as (select vin, time_value, g_status, a_rectime, dt
           from ods_analysis_data),
     b as (select vin, dt, max(time_value) as idle_start_time
           from a
           where g_status = 1
           group by vin, dt),
     c as (select a.vin, min(time_value) as idle_end_time
           from a
                    join b on a.vin = b.vin and a.g_status = 1
           where a.time_value > b.idle_start_time
           group by a.vin),
     d as (select b.dt, b.vin, b.idle_start_time, c.idle_end_time
           from b
                    join c on b.vin = c.vin
           where b.idle_start_time < c.idle_end_time)
select *
from d;
--处理完时间不对的问题,继续验证,写插入分区问题.
