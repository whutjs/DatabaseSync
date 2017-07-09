package com.alibaba.middleware.race.sync;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

// 表示记录中的一列
class Column implements Comparable{
    // 在第几列
    public int columnNO;
    // 是否是字符串
    public boolean isStr;
//    // 列名
//    public String columnName;
    public String strColumnVal;
    public long longColumnVal;

    @Override
    public int compareTo(Object o) {
        Column c2 = (Column)o;
        return Integer.compare(this.columnNO, c2.columnNO);
    }
}
/**
 * 表示一条记录
 * Created by Jenson on 2017/6/8.
 */
public class Record implements Comparable{
    // 主键
    private Long pk;
    private HashMap<String, Column> columnsMap;

    public Record() {
        this.columnsMap = new HashMap<>();
    }

    public Record(Long p) {
        this.pk = p;
        this.columnsMap = new HashMap<>();
    }

    public Long getPK() {
        return this.pk;
    }
    public void setPK(Long p) {
        this.pk = p;
    }

    public void addColumn(final String columnName, final Column cl) {
        this.columnsMap.put(columnName, cl);
    }

    public Column getColumn(final String columnName) {
        return this.columnsMap.get(columnName);
    }

    public Set<String> getColumnNameSet() {
        return this.columnsMap.keySet();
    }

    @Override
    public int compareTo(Object o) {
        Record rec = (Record)o;
        return this.getPK().compareTo(rec.getPK());
    }
}
