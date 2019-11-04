# flink-demo
基于flink的练习

* flink sql client不支持CREATE TABLE的DDL语句，Table api是支持。
可以自己写一个提交sql的jar包练习下，学习如何使用flink sql。


* 发现一位大神写了一个提交sql作业的脚本，可以直接使用，地址：
[https://github.com/wuchong/flink-sql-submit](https://github.com/wuchong/flink-sql-submit)


* flink kafka consumer根据时间消费kafka信息

[todo]kafka消费消息几个场景:
- 从当前时间往前推2个小时开始接着消费,消费完这2个小时断开
- 从当前时间往前推2个小时开始接着持续消费
- 指定个时间戳开始持续消费
- 指定个开始时间与结束时间消费,消费完断开