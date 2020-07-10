package com.persagy.hbase.flow.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 总量实体类
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class HTableTotal implements Comparable<HTableTotal> {

    //展示时间点
    private String flowTime;

    //汇总的数据总量
    private Long total;

    @Override
    public int compareTo(HTableTotal o) {
        int i = o.getFlowTime().compareTo(o.getFlowTime());
        return 0;
    }
}
