package com.persagy.hbase.flow.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableInfo {

    //表id
    private String tableId;

    //表名
    private String tableName;

    //归属数据库
    private String dbName;

    //读行数
    private Long readLines;

    //读字节数
    private Long readBytes;

    //插入行数
    private Long insertLines;

    //插入字节数
    private Long insertBytes;

    //更新行数
    private Long updateLines;

    //更新字节数
    private Long updateBytes;

    //删除行数
    private Long deleteLines;

    //删除字节数
    private Long deleteBytes;

    //操作时间
    private Long operationTime;

    //时间标记
    private String pointTime;

    public void initValue(){
        this.tableId = "";
        this.tableName = "";
        this.dbName = "";
        this.readLines = 0L;
        this.readBytes = 0L;
        this.insertLines = 0L;
        this.insertBytes = 0L;
        this.updateLines = 0L;
        this.updateBytes = 0L;
        this.deleteLines = 0L;
        this.deleteBytes = 0L;
        this.operationTime = 0L;
        this.pointTime = "";
    }
}
