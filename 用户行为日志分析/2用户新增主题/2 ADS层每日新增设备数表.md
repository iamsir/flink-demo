每日新增设备表

1)建表
```
drop table if exists  ads_new_mid_count;
create external  table  ads_new_mid_count
(
    `create_date`     string comment '创建时间',
    `new_mid_count`   BIGINT comment '新增设备数量'
)  COMMENT '每日新增设备信息数量'
row format delimited  fields terminated by '\t'
location '/warehouse/gmall/ads/ads_new_mid_count/';
```
2)导入数据
```
insert into table ads_new_mid_count
select
    create_date ,
    count(*)
from dws_new_mid_day
where create_date='2019-02-10'
group by create_date ;
```
