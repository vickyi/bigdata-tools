package com.aiplus.bi.etl.input.rdb;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @author dev
 */
public class Table {

    public static final Comparator<ColumnList.Column> COLUMN_ASC = Comparator.comparingInt(ColumnList.Column::getIndex);

    private String name;

    private String target;

    private String splitKey;

    private ColumnList columnList;

    private long start;

    private long end;

    private long length;

    private String[] keyNames;

    private String[] columnNames;

    public Table(String name, String target, String splitKey, ColumnList columnList) {
        this.name = name;
        this.target = target;
        this.splitKey = splitKey;
        this.columnList = columnList;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ColumnList getColumnList() {
        return columnList;
    }

    public void setColumnList(ColumnList columnList) {
        this.columnList = columnList;
    }

    public void sortColumns(Comparator<ColumnList.Column> comparator) {
        this.columnList.sort(comparator);
    }

    public void prepared() {
        List<String> pks = new ArrayList<>();
        List<String> cols = new ArrayList<>();
        for (ColumnList.Column key : getColumnList().getKeys()) {
            pks.add(key.getName());
        }
        this.keyNames = pks.toArray(new String[0]);

        for (ColumnList.Column column : getColumnList().getColumns()) {
            cols.add(column.getName());
        }
        this.columnNames = cols.toArray(new String[0]);
    }

    public String getSplitKey() {
        return splitKey;
    }

    public void setSplitKey(String splitKey) {
        this.splitKey = splitKey;
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    public void setLengthInterval(long start, long end) {
        this.start = start;
        this.end = end;
        this.length = end - start;
    }

    public long getLength() {
        return length;
    }

    public String[] getKeyNames() {
        return keyNames;
    }

    public String[] getColumnNames() {
        return columnNames;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }
}
