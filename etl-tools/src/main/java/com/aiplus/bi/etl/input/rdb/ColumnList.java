package com.aiplus.bi.etl.input.rdb;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @author dev
 */
public class ColumnList {

    private List<String> names;

    private List<Column> keys;

    private List<Column> columns;

    /**
     * 表示配置中的字段，如果有就按照这个字段的顺序最终输出结果
     */
    private String[] fields;

    public ColumnList(String[] fields) {
        this.fields = fields;
        this.names = new ArrayList<>();
        this.keys = new ArrayList<>(5);
        this.columns = new ArrayList<>();
    }

    public boolean hasFields() {
        return this.fields != null;
    }

    public List<String> getNames() {
        return names;
    }

    public List<Column> getKeys() {
        return keys;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public void addColumn(String name, int dataType, int index, boolean isPrimaryKey) {
        this.names.add(name);
        if (isPrimaryKey) {
            this.keys.add(new Column(name, dataType, index, true));
        } else {
            this.columns.add(new Column(name, dataType, index, false));
        }
    }

    public void sort(Comparator<Column> comparator) {
        keys.sort(comparator);
        columns.sort(comparator);
        // 名称也要排序
        this.names.clear();
        // 先清空，再重新添加
        for (Column key : keys) {
            this.names.add(key.getName());
        }
        for (Column column : columns) {
            this.names.add(column.getName());
        }
    }

    public boolean containsColumnByName(String name) {
        return this.names.contains(name);
    }

    public static class Column {
        private String name;
        private int dataType;
        private int index;
        private boolean primaryKey;

        public Column(String name, int dataType, int index, boolean primaryKey) {
            this.name = name;
            this.dataType = dataType;
            this.index = index;
            this.primaryKey = primaryKey;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getDataType() {
            return dataType;
        }

        public void setDataType(int dataType) {
            this.dataType = dataType;
        }

        public int getIndex() {
            return index;
        }

        public void setIndex(int index) {
            this.index = index;
        }

        public boolean isPrimaryKey() {
            return primaryKey;
        }

        public void setPrimaryKey(boolean primaryKey) {
            this.primaryKey = primaryKey;
        }
    }
}
