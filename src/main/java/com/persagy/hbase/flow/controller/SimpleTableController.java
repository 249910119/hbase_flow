package com.persagy.hbase.flow.controller;

import com.alibaba.fastjson.JSONObject;
import com.persagy.hbase.flow.bean.HTableTotal;
import com.persagy.hbase.flow.service.SimpleTableService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 单表数据统计
 */
@RestController
public class SimpleTableController {

    @Autowired
    SimpleTableService simpleTableService;

    /**
     * 通过首页点击查看表详情以及单表详情页按日期、下拉列表获取表的数据情况
     * @param tableName 表名
     * @param startDate 开始日期
     * @param endDate 结束日期
     * @param optionType 下拉框选择条件（默认查询总数据量?）
     * @return
     */
    @GetMapping("simple_table_total")
    public String getSimpleTableInfo(@RequestParam("table_name") String tableName,
                                         @RequestParam("start_date") String startDate,
                                         @RequestParam("end_date") String endDate,
                                         @RequestParam("option_type") String optionType){
        JSONObject result = simpleTableService.getSimpleTable(startDate, endDate, optionType, tableName);

        return result.toJSONString();
    }

    @GetMapping("list_sort")
    public List<HTableTotal> getStringSort(){

        List<HTableTotal> l = new ArrayList<>();

        l.add(new HTableTotal("202007071130", 54513215L));
        l.add(new HTableTotal("202007071125", 54513215L));
        l.add(new HTableTotal("202007071140", 54513215L));
        l.add(new HTableTotal("202007071135", 54513215L));

        Collections.sort(l);

        return l;
    }
}
