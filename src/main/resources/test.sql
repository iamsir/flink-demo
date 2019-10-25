# source
CREATE TABLE start_log_source( 
   mid_id VARCHAR,  
   user_id INT,  
   version_code VARCHAR,  
   version_name VARCHAR,  
   lang VARCHAR,  
   source VARCHAR,  
   os VARCHAR,  
   area VARCHAR,  
   model VARCHAR,  
   brand VARCHAR,  
   sdk_version VARCHAR,  
   height_width VARCHAR,  
   app_time TIMESTAMP,
   network VARCHAR,  
   lng FLOAT,  
   lat FLOAT  
) WITH ( 
   'connector.type' = 'kafka',  
   'connector.version' = '0.11',
   'connector.topic' = 'start_log',  
   'connector.startup-mode' = 'earliest-offset',  
   'connector.properties.0.key' = 'zookeeper.connect',  
   'connector.properties.0.value' = 'localhost:2181',  
   'connector.properties.1.key' = 'bootstrap.servers',  
   'connector.properties.1.value' = 'localhost:9092',  
   'update-mode' = 'append',  
   'format.type' = 'json',  
   'format.derive-schema' = 'true'  
);

# sink
CREATE TABLE start_log_sink ( 
    mid_id VARCHAR, 
    user_id INT,
    event_time_test TIMESTAMP
) WITH ( 
    'connector.type' = 'jdbc', 
    'connector.url' = 'jdbc:mysql://localhost:3306/flink_test',
    'connector.table' = 'start_log_to_mysql', 
    'connector.username' = 'root', 
    'connector.password' = 'Aa123456', 
    'connector.write.flush.max-rows' = '1' 
);

insert into start_log_sink 
select mid_id, user_id, app_time
from start_log_source;
