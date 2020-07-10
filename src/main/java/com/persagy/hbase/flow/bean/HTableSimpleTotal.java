package com.persagy.hbase.flow.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class HTableSimpleTotal {

    private String tableName;

    private String startDate;

    private String endDate;

    private Long total;
}
