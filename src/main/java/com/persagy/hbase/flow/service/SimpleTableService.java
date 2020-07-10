package com.persagy.hbase.flow.service;

import com.alibaba.fastjson.JSONObject;

public interface SimpleTableService {

    /**
     * 获取单表数据统计
     * @param startDate
     * @param endDate
     * @param optionType
     * @param tableName
     * @return
     */
    public JSONObject getSimpleTable(String startDate, String endDate, String optionType, String tableName);

}
