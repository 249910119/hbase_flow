package com.persagy.hbase.flow.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.persagy.hbase.flow.constant.HbaseDBConstant;
import com.persagy.hbase.flow.service.TableTotalService;
import com.persagy.hbase.flow.utils.CommonUtils;
import com.persagy.hbase.flow.utils.HbaseUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class TableTotalServiceImpl implements TableTotalService {

    @Override
    public JSONObject getCollectDatas(String startDate, String endDate, String optionType) {

        long b = System.currentTimeMillis();
        JSONObject jsonObject = new JSONObject();
        JSONObject allTableJsonObject = new JSONObject();
        JSONObject simpleTableJsonObject = new JSONObject();

        long b1 = System.currentTimeMillis();
        Connection connection = HbaseUtils.connection;
        long e1 = System.currentTimeMillis();
        System.out.println("获取连接时间：" + (e1 - b1));

        long b2 = System.currentTimeMillis();
        Map<String, String> rowKeyFilterMap = new HashMap<>();
        rowKeyFilterMap.put(startDate, endDate);

        Filter rowKeyFilter = HbaseUtils.getRowKeyFilter(rowKeyFilterMap);
        long e2 = System.currentTimeMillis();
        System.out.println("获取过滤器时间：" + (e2 - b2));

        long b3 = System.currentTimeMillis();
        List<String> queryTableNames = HbaseUtils.getQueryTableName(connection,
                startDate, endDate, HbaseDBConstant.ZILLION_META_STAT_2, null);
        long e3 = System.currentTimeMillis();
        System.out.println("获取要查询的表时间：" + (e3 - b3));


        long b4 = System.currentTimeMillis();
        for (String queryTableName : queryTableNames) {

            ResultScanner resultScanner = HbaseUtils.getResultScanner(connection,
                    queryTableName, optionType, rowKeyFilter);

            String dbName = queryTableName.split(":")[0];

            if (resultScanner == null) continue;

            for (Result result : resultScanner) {

                List<Cell> cells = result.listCells();

                if (cells != null && cells.size() > 0){

                    for (Cell cell : cells) {

                        String rowKeys = Bytes.toString(CellUtil.cloneRow(cell));
                        String[] split = rowKeys.split(",");
                        String flowTime = split[0];
                        String tableName = split[1];

                        byte[] cellBytes = CellUtil.cloneValue(cell);
                        Long allTableValue = 0L;
                        if (cellBytes.length > 0) {
                            allTableValue = Bytes.toLong(cellBytes);
                        }

                        if (allTableJsonObject.get(flowTime) == null){
                            allTableJsonObject.put(flowTime, allTableValue);
                        } else {
                            Long r = (Long) allTableJsonObject.get(flowTime) + allTableValue;
                            allTableJsonObject.put(flowTime, r);
                        }


                        String subTableName = CommonUtils.subTableName(tableName);

                        String key = dbName + ":" + subTableName;

                        Long simpleTableValue = 0L;
                        if (cellBytes.length > 0) {
                            simpleTableValue = Bytes.toLong(cellBytes);
                        }

                        if (simpleTableJsonObject.get(key) == null){
                            simpleTableJsonObject.put(key, simpleTableValue);
                        } else {
                            Long r = (Long) simpleTableJsonObject.get(key) + simpleTableValue;
                            simpleTableJsonObject.put(key, r);
                        }
                    }
                }
            }
        }
        long e4 = System.currentTimeMillis();
        System.out.println("循环处理数据时间：" + (e4 - b4));

        jsonObject.put("all_table_count", allTableJsonObject);
        jsonObject.put("simple_list_count", simpleTableJsonObject);

        long e = System.currentTimeMillis();

        System.out.println("首页处理数据总时间：" + (e - b));

        return jsonObject;
    }

    @Override
    public JSONObject getCollectDataByAllTable(String startDate, String endDate, String optionType) {

        JSONObject jsonObject = new JSONObject();

        Connection connection = HbaseUtils.connection;

        Map<String, String> rowKeyFilterMap = new HashMap<>();
        rowKeyFilterMap.put(startDate, endDate);

        Filter rowKeyFilter = HbaseUtils.getRowKeyFilter(rowKeyFilterMap);

        List<String> queryTableNames = HbaseUtils.getQueryTableName(connection,
                startDate, endDate, HbaseDBConstant.ZILLION_META_STAT_2, null);

        for (String queryTableName : queryTableNames) {

            ResultScanner resultScanner = HbaseUtils.getResultScanner(connection, queryTableName, optionType, rowKeyFilter);

            if (resultScanner == null) continue;

            for (Result result : resultScanner) {

                List<Cell> cells = result.listCells();

                if (cells != null && cells.size() > 0){

                    for (Cell cell : cells) {

                        String rowKeys = Bytes.toString(CellUtil.cloneRow(cell));
                        String flowTime = rowKeys.split(",")[0];

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
        }

        return jsonObject;
    }

    @Override
    public JSONObject getCollectDataBySimpleTable(String startDate, String endDate, String optionType) {

        JSONObject jsonObject = new JSONObject();

        Connection connection = HbaseUtils.connection;

        Map<String, String> rowKeyFilterMap = new HashMap<>();
        rowKeyFilterMap.put(startDate, endDate);

        Filter rowKeyFilter = HbaseUtils.getRowKeyFilter(rowKeyFilterMap);

        List<String> queryTableNames = HbaseUtils.getQueryTableName(connection,
                startDate, endDate, HbaseDBConstant.ZILLION_META_STAT_2, null);

        for (String queryTableName : queryTableNames) {

            ResultScanner resultScanner = HbaseUtils.getResultScanner(connection, queryTableName, optionType, rowKeyFilter);

            String dbName = queryTableName.split(":")[0];

            if (resultScanner == null) continue;

            for (Result result : resultScanner) {

                List<Cell> cells = result.listCells();

                if (cells != null && cells.size() > 0){

                    for (Cell cell : cells) {

                        String rowKeys = Bytes.toString(CellUtil.cloneRow(cell));
                        String tableName = rowKeys.split(",")[1];

                        String subTableName = CommonUtils.subTableName(tableName);

                        String key = dbName + ":" + subTableName;

                        byte[] cellBytes = CellUtil.cloneValue(cell);
                        Long value = 0L;
                        if (cellBytes.length > 0) {
                            value = Bytes.toLong(cellBytes);
                        }

                        if (jsonObject.get(key) == null){
                            jsonObject.put(key, value);
                        } else {
                            Long r = (Long) jsonObject.get(key) + value;
                            jsonObject.put(key, r);
                        }
                    }
                }
            }
        }

        return jsonObject;
    }


}
