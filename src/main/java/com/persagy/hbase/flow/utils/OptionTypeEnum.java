package com.persagy.hbase.flow.utils;

/**
 * 汇总数据类型枚举
 */
public enum OptionTypeEnum {

    //总数据量
    datas_total("1","read_bytes,insert_bytes,update_bytes,delete_bytes"),
    //读数据量
    read_datas_total("2","read_bytes"),
    //写数据量
    write_datas_total("3","insert_bytes,update_bytes,delete_bytes"),
    //增数据量
    add_datas_total("4","insert_bytes"),
    //改数据量
    alter_datas_total("5","update_bytes"),
    //删数据量
    delete_datas_total("6","delete_bytes"),
    //总行数
    lines_total("7","read_lines,insert_lines,update_lines,delete_lines"),
    //读行数
    read_lines_total("8","read_lines"),
    //写行数
    write_lines_total("9","insert_lines,update_lines,delete_lines"),
    //增行数
    add_lines_total("10","insert_lines"),
    //改行数
    alter_lines_total("11","update_lines"),
    //删行数
    delete_lines_total("12","delete_lines");

    private String index;
    private String name;


    OptionTypeEnum(String index, String name) {
        this.index = index;
        this.name = name;
    }


    public static String getName(String index){

        for(OptionTypeEnum optionTypeEnum : OptionTypeEnum.values()){
            if(optionTypeEnum.getIndex().equals(index)){
                return optionTypeEnum.name;
            }
        }
        return "";
    }


    public String getIndex() {
        return index;
    }


    public String getName() {
        return name;
    }
}
