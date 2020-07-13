package com.persagy.hbase.flow.controller;

import com.alibaba.fastjson.JSONObject;
import com.persagy.hbase.flow.service.TableTotalService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 首页数据统计
 */
@RestController
public class TableTotalController {

    @Autowired
    TableTotalService tableTotalService;

    @GetMapping("table_total_list")
    public String getTableTotal(@RequestParam("start_date") String startDate,
                                @RequestParam("end_date") String endDate,
                                @RequestParam("option_type") String optionType){
        JSONObject json = tableTotalService.getCollectDatas(startDate, endDate, optionType);

        return json.toJSONString();
    }

    /**
     * 首页按日期、选择条件统计数据（饼图、柱状图）
     * @param startDate 开始日期
     * @param endDate 结束日期
     * @param optionType 下拉框选择条件（默认查询总数据量?）
     * @return
     */
    @GetMapping("table_total_chart")
    public String getAllTableTotalToChart(@RequestParam("start_date") String startDate,
                              @RequestParam("end_date") String endDate,
                              @RequestParam("option_type") String optionType){

        JSONObject result = tableTotalService.getCollectDataBySimpleTable(startDate, endDate, optionType);

        return result.toJSONString();
    }

    /**
     * 总数据，按分钟聚合
     * @param startDate 开始日期
     * @param endDate 结束日期
     * @param optionType 下拉框选择条件
     * @return
     */
    @GetMapping("table_total")
    public String getAllTableTotal(@RequestParam("start_date") String startDate,
                                       @RequestParam("end_date") String endDate,
                                       @RequestParam("option_type") String optionType){
        JSONObject result = tableTotalService.getCollectDataByAllTable(startDate, endDate, optionType);

        return result.toJSONString();
    }


}
