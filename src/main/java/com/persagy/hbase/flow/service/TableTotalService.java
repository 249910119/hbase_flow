package com.persagy.hbase.flow.service;

import com.alibaba.fastjson.JSONObject;

public interface TableTotalService {

    /**
     * 起止日期内,符合 optionType 数据汇总
     * @param startDate
     * @param endDate
     * @param optionType
     * @return
     */
    public JSONObject getCollectDataByAllTable(String startDate, String endDate, String optionType);

    /**
     * 按表汇总数据
     * @param startDate
     * @param endDate
     * @param optionType
     * @return
     */
    public JSONObject getCollectDataBySimpleTable(String startDate, String endDate, String optionType);
}
