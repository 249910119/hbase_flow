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
    public JSONObject getCollectDataByAllTable(String startDate, String endDate, String optionType) {

        JSONObject jsonObject = new JSONObject();

        Connection connection = HbaseUtils.connection;

        Map<String, String> rowKeyFilterMap = new HashMap<>();
        rowKeyFilterMap.put(startDate, endDate);

        Filter rowKeyFilter = HbaseUtils.getRowKeyFilter(rowKeyFilterMap);

        List<String> queryTableNames = HbaseUtils.getQueryTableName(connection, startDate, endDate, HbaseDBConstant.ZILLION_META_STAT_2, null);

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

        List<String> queryTableNames = HbaseUtils.getQueryTableName(connection, startDate, endDate, HbaseDBConstant.ZILLION_META_STAT_2, null);

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
