package com.zhdan.flink.cli;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * @author dongan.zhang
 * @date 2019-10-22
 **/
@Slf4j
public class SqlSubmit {

    /**
     * run flink sql
     * @param sqls
     * @throws Exception
     */
    public void run(List<String> sqls)throws Exception {

        final EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        final TableEnvironment tableEnv = TableEnvironment.create(settings);
        //sql文本中只有create、insert语句
        for (String sql : sqls) {
            tableEnv.sqlUpdate(sql);
        }

        tableEnv.execute("submit sql job");

    }

    public static void main(String[] args) throws Exception {

        if (args == null || args.length < 1) {
            throw new IllegalArgumentException("缺少<sql-file>");
        }
        //获取sql-file的全路径名
        String fileName = args[0];

        //解析内容，获取sql语句
        List<String> sqlLines = Files.readAllLines(Paths.get(fileName));
        List<String> sqls = parse(sqlLines);
        //提交sql job
        new SqlSubmit().run(sqls);
    }


    private static List<String> parse(List<String> sqlLines) {
        List<String> sqlList = new ArrayList<>();
        StringBuilder stmt = new StringBuilder();
        for (String line : sqlLines) {

            if (line.trim().isEmpty() || line.trim().startsWith("#")) {
                // 过滤掉空行以及注释行
                continue;
            }
            stmt.append("\n").append(line.trim());
            if (line.trim().endsWith(";")) {
                sqlList.add(stmt.substring(0, stmt.length() - 1).trim());
                //清空，复用
                stmt.setLength(0);
            }
        }
        return sqlList;
    }

}
