package com.persagy.hbase.flow.utils;

import com.persagy.hbase.flow.constant.HbaseDBConstant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Hbase 工具类
 */
public class HbaseUtils {

    public static Connection connection = null;
//    public static ResultScanner scanner = null;
//    public static Table htable =  null;

    /**
     * 初始化 Hbase 连接
     */
    static{

        Configuration conf = HBaseConfiguration.create();

        conf.set(HbaseDBConstant.HBASE_ZOOKEEPER_QUORUM, "zookeeper1");
        conf.set(HbaseDBConstant.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT, HbaseDBConstant.HBASE_ZOOKEEPER_PORT);

        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

/*

    public static Connection getConnection(){

        Configuration conf = HBaseConfiguration.create();

        conf.set(HbaseDBConstant.HBASE_ZOOKEEPER_QUORUM, HbaseDBConstant.HBASE_ZOOKEEPER_IP);
        conf.set(HbaseDBConstant.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT, HbaseDBConstant.HBASE_ZOOKEEPER_PORT);

        try {
            //newCachedThreadPool
            ExecutorService executorService = Executors.newFixedThreadPool(10);

            connection = ConnectionFactory.createConnection(conf, executorService);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return connection;
    }
*/

    /**
     * 获取所有 nameSpace 名字
     * @param connection
     * @return
     */
    public static List<String> getAllNameSpace(Connection connection){

        List<String> nameSpaceList = new ArrayList<>();

        try {

            Admin admin = connection.getAdmin();

            NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();

            for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors) {
                String name = namespaceDescriptor.getName();
                if ("default".equals(name) || "hbase".equals(name)) {
                    continue;
                }
                nameSpaceList.add(name);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return nameSpaceList;
    }

    /**
     * rowKey 过滤
     * @param rowKeyFilterMap key:startKey ; value:endKey
     * @return
     */
    public static Filter getRowKeyFilter(Map<String, String> rowKeyFilterMap){

        Filter rowKeyFilter = null;
        List<MultiRowRangeFilter.RowRange> rangeList_new = new ArrayList<MultiRowRangeFilter.RowRange>();

        try {

            for (Map.Entry<String, String> filterKeys : rowKeyFilterMap.entrySet()) {

                byte[] start = Bytes.toBytes(filterKeys.getKey());
                byte[] end = Bytes.toBytes(filterKeys.getValue());

                MultiRowRangeFilter.RowRange rowRange = new MultiRowRangeFilter.RowRange(start, true, end, true);

                rangeList_new.add(rowRange);
            }

            rowKeyFilter = new MultiRowRangeFilter(rangeList_new);

        } catch (IOException e) {
            e.printStackTrace();
        }

        return rowKeyFilter;
    }

    /**
     * 查询结果
     * @param queryTableName 查询的表
     * @param optionType 下拉框选择查询的数据
     * @param rowKeyFilter rowKey 过滤器
     * @return
     */
    public static ResultScanner getResultScanner(Connection connection,
                                                 String queryTableName,
                                                 String optionType,
                                                 Filter rowKeyFilter
                                                 ) {
        ResultScanner scanner = null;
        try {

            Admin admin = connection.getAdmin();
            boolean b = admin.tableExists(TableName.valueOf(queryTableName));

            if (b) {

                Table htable = connection.getTable(TableName.valueOf(queryTableName));

                Scan scan = new Scan();

                //添加rowKey过滤
                if (rowKeyFilter != null){
                    scan.setFilter(rowKeyFilter);
                }

                //添加列族过滤或者列过滤
                String optionTypeNames = OptionTypeEnum.getName(optionType);
                if (optionTypeNames == null || optionTypeNames.isEmpty()) {
                    return null;
                }

                scan.addFamily(Bytes.toBytes(HbaseDBConstant.FAMILY_NAME));

                for (String columnName : optionTypeNames.split(",")) {
                    scan.addColumn(Bytes.toBytes(HbaseDBConstant.FAMILY_NAME),Bytes.toBytes(columnName));
                }

                scanner = htable.getScanner(scan);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return scanner;
    }

    /**
     * 获取数据表集合
     * @param name
     * @param connection
     * @return
     */
    public static List<String> getHTableNameList(String name, Connection connection){

        List<String> list = null;
        if (name != null && !name.isEmpty()){
            list = new ArrayList<>();
            try {
                String[] names = name.split(":");

                Admin admin = connection.getAdmin();
                Pattern pattern = Pattern.compile(names[0] + ":.+" + names[1] + ".+");

//                TableName[] tableNames = admin.listTableNamesByNamespace(names[0]);
                TableName[] tableNames = admin.listTableNames(pattern);
                for (TableName tableName : tableNames) {
                    String v = Bytes.toString(tableName.getName());
                    if (v.contains(names[1])){
                        list.add(v);
                    }
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return list;
    }

    /**
     * 拼接要查询的数据库
     * @param connection
     * @param startDate
     * @param endDate
     * @return
     */
    public static List<String> getQueryTableName(Connection connection, String startDate, String endDate, String tableName, String dbName){

        List<String> monthDiffList = DateUtils.getDateMonthDiff(startDate, endDate);

        List<String> queryTableNames = new ArrayList<>();

        if (monthDiffList != null && monthDiffList.size() > 0) {

            for (String month : monthDiffList) {

                List<String> allNameSpace = HbaseUtils.getAllNameSpace(connection);

                for (String nameSpace : allNameSpace) {

                    if (dbName == null){

                        String queryTableName = nameSpace + ":" + tableName + month;
                        queryTableNames.add(queryTableName);

                    }else {

                        if (nameSpace.equals(dbName)){
                            String queryTableName = nameSpace + ":" + tableName + month;
                            queryTableNames.add(queryTableName);
                        }
                    }
                }
            }
        }

        return queryTableNames;
    }

    /**
     * 关闭连接
     */
    public static void closeHbaseConnection(){

    }

}
