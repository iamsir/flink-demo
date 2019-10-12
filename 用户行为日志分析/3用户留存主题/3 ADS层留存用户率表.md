留存比率

1)建表
```
drop table if exists  ads_user_retention_day_rate;
create external  table  ads_user_retention_day_rate 
(
     `stat_date`         string comment '统计日期',
     `create_date`       string  comment '设备新增日期',
     `retention_day`     int comment '截止当前日期留存天数',
     `retention_count`   bigint comment  '留存数量',
     `new_mid_count`     bigint  comment '当日设备新增数量',
     `retention_ratio`   decimal(10,2) comment '留存率'
)  COMMENT '每日用户留存情况'
row format delimited fields terminated by '\t‘
location '/warehouse/gmall/ads/ads_user_retention_day_rate/';

```
2)导入数据
```
insert into table ads_user_retention_day_rate
select 
    '2019-02-11' , 
    ur.create_date,
    ur.retention_day, 
    ur.retention_count , 
    nc.new_mid_count,
    ur.retention_count/nc.new_mid_count*100
from 
(
    select   
        create_date, 
        retention_day, 
        count(*) retention_count  
    from dws_user_retention_day 
    where dt='2019-02-11' 
    group by create_date,retention_day
)  ur join ads_new_mid_count nc on nc.create_date=ur.create_date;

```
