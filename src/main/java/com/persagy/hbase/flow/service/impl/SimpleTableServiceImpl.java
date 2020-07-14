package com.persagy.hbase.flow.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.persagy.hbase.flow.constant.HbaseDBConstant;
import com.persagy.hbase.flow.service.SimpleTableService;
import com.persagy.hbase.flow.utils.DateUtils;
import com.persagy.hbase.flow.utils.HbaseUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class SimpleTableServiceImpl implements SimpleTableService {

    @Override
    public JSONObject getSimpleTable(String startDate, String endDate, String optionType, String tableName) {

        JSONObject jsonObject = new JSONObject();

        long l = System.currentTimeMillis();
        Connection connection = HbaseUtils.connection;

        long l1 = System.currentTimeMillis();
        System.out.println("get connect :" + (l1 - l));

        Map<String, String> rowKeyFilterMap = new HashMap<>();

        List<String> hTableNameList = HbaseUtils.getHTableNameList(tableName, connection);
        long l2 = System.currentTimeMillis();
        System.out.println("获取列表名：" + (l2 - l1));

        for (String hTableName : hTableNameList) {
            String[] v = hTableName.split(":");
            String startRowKey = v[1] + "," + DateUtils.getDateMinute(startDate) + "00";
            String endRowKey = v[1] + "," + DateUtils.getDateMinute(endDate) + "00";
            rowKeyFilterMap.put(startRowKey, endRowKey);
        }

        Filter rowKeyFilter = HbaseUtils.getRowKeyFilter(rowKeyFilterMap);

        List<String> monthDiffList = DateUtils.getDateMonthDiff(startDate, endDate);

        long l3 = System.currentTimeMillis();
        List<String> queryTableNames = HbaseUtils.getQueryTableName(connection, startDate, endDate, HbaseDBConstant.ZILLION_META_STAT_1, tableName.split(":")[0]);
        long l4 = System.currentTimeMillis();
        System.out.println("获取要查询的表 : " + (l4 - l3));

        for (String queryTableName : queryTableNames) {

            if (monthDiffList != null && monthDiffList.size() > 0){

                long l5 = System.currentTimeMillis();
                ResultScanner resultScanner = HbaseUtils.getResultScanner(connection, queryTableName, optionType, rowKeyFilter);

                long l6 = System.currentTimeMillis();
                System.out.println(queryTableName + "获取数据时间:" + (l6 -l5));

                if (resultScanner == null) continue;

                long l7 = System.currentTimeMillis();
                for (Result result : resultScanner) {

                    List<Cell> cells = result.listCells();

                    if (cells != null && cells.size() > 0){

                        for (Cell cell : cells) {

                            String rowKeys = Bytes.toString(CellUtil.cloneRow(cell));

                            String s = rowKeys.split(",")[0];

                            String flowTime = rowKeys.split(",")[1];

                            byte[] cellBytes = CellUtil.cloneValue(cell);
                            Long value = 0L;
                            if (cellBytes.length > 0) {
                                value = Bytes.toLong(cellBytes);
                            }

                            if (jsonObject.get(flowTime) == null){
                                jsonObject.put(flowTime, value);
                            } else {
                                Long r = (Long) jsonObject.get(flowTime) + value;
                                jsonObject.put(flowTime, r);
                            }
                        }
                    }
                }

                long l8 = System.currentTimeMillis();
                System.out.println(queryTableName + "计算数据时间" +  (l8 - l7));
            }
        }
        long l5 = System.currentTimeMillis();
        System.out.println("处理数据时间：" + (l5 - l4));

        long l6 = System.currentTimeMillis();
        TableName name=TableName.valueOf(tableName);
        Scan scan = new Scan();
        AggregationClient aggregationClient = new AggregationClient(connection.getConfiguration());
        try {
            long tableCount = aggregationClient.rowCount(name, new LongColumnInterpreter(), scan);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        long l7 = System.currentTimeMillis();
        System.out.println("获取表行数时间：" + (l5 - l4));



        return jsonObject;
    }
}
