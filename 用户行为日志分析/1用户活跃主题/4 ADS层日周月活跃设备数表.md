活跃设备分析

1)建表
```
drop table if exists ads_uv_count;
create  external table ads_uv_count(
    `dt` string COMMENT '统计日期',
    `day_count` bigint COMMENT '当日用户数量',
    `wk_count`  bigint COMMENT '当周用户数量',
    `mn_count`  bigint COMMENT '当月用户数量',
    `is_weekend` string COMMENT 'Y,N是否是周末,用于得到本周最终结果',
    `is_monthend` string COMMENT 'Y,N是否是月末,用于得到本月最终结果'
) COMMENT '每日活跃用户数量'
row format delimited fields terminated by '\t‘
location '/warehouse/gmall/ads/ads_uv_count_day/'
;
```
2)导入数据
```
insert  into table ads_uv_count
select
  '2019-02-10' dt,
   daycount.ct,
   wkcount.ct,
   mncount.ct,
   if(date_add(next_day('2019-02-10','MO'),-1)='2019-02-10','Y','N') ,
   if(last_day('2019-02-10')='2019-02-10','Y','N')
from
(
   select
      '2019-02-10' dt,
       count(*) ct
   from dws_uv_detail_day
   where dt='2019-02-10'
)daycount   join
(
   select
     '2019-02-10' dt,
     count (*) ct
   from dws_uv_detail_wk
   where wk_dt=concat(date_add(next_day('2019-02-10','MO'),-7),'_' ,date_add(next_day('2019-02-10','MO'),-1) )
)  wkcount  on daycount.dt=wkcount.dt
join
(
   select
     '2019-02-10' dt,
     count (*) ct
   from dws_uv_detail_mn
   where mn=date_format('2019-02-10','yyyy-MM')
)mncount on daycount.dt=mncount.dt;
```