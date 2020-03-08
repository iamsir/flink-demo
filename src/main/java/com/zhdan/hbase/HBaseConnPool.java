package com.zhdan.hbase;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

/**
 * @author zhdan
 * @date 2020-03-07
 */
@Slf4j
public class HBaseConnPool implements Serializable {

    private static Configuration conf = null;

    //HBase中Connection已经实现了对连接的管理功能,是线程安全的，一个JVM中共用一个Connection对象即可，
    // 而Table和Admin则不是线程安全的不同的线程中使用单独的Table和Admin对象。
    private static Connection conn = null;

    private static final HBaseConnPool instance = new HBaseConnPool();

    private HBaseConnPool() {
        log.info("init HBaseConnPool");
    }

    /**
     * 获取pool实例
     * @return HBaseConnPool
     */
    public static HBaseConnPool getInstance(){
        log.info("execute getInstance");
        if(instance == null){
            log.error("getInstance error, instance is null.");
        }
        return instance;
    }

    public synchronized Connection getConnection() throws Exception {
        log.info("get connection");
        if (conn == null || conn.isClosed()) {
            log.info("recreate HBase connection");
            closeConnectionPool();
            createConnectionPool();
        }
        if (conn == null) {
            log.error("getConnection error, conn is null");
        }
        return conn;
    }

    private void createConnectionPool() throws IOException {
        log.info("create connection pool");
        loadConfiguration();
        conn = ConnectionFactory.createConnection(conf);
    }

    private void closeConnectionPool() throws IOException {
        log.info("close connection pool");
        if (conn == null) {
            log.info("conn is null, end close");
            return;
        }
        conn.close();
        log.info("end close");
    }

    /**
     * 加载配置文件
     */
    private void loadConfiguration(){
        conf = HBaseConfiguration.create();
        //获取路径:classes/
        String filePath = this.getClass().getClassLoader().getResource("").getPath();
        String hbaseFilePath = filePath + "hbase" + File.separator;

        //读取hbase客户端配置文件
        conf.addResource(new Path(hbaseFilePath + "core-site.xml"));
        conf.addResource(new Path(hbaseFilePath + "hbase-site.xml"));
    }
}
