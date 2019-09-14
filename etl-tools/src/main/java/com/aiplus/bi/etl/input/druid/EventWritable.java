package com.aiplus.bi.etl.input.druid;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.Map;
import java.util.TimeZone;

/**
 * @author dev
 */
public class EventWritable {

    private static final String COLUMN_SPLIT = "\001";

    private static final String HIVE_NULL_VALUE = "\\N";

    private long timestamp;

    private int year;

    private int month;

    private int day;

    private int hour;

    private int minute;

    private String dataText;

    private String dataSource;

    private String interval;

    public void readFields(String dataSource, String interval, DateTime dts, Map<String, Object> dataMap) {
        this.dataSource = dataSource;
        this.interval = interval;
        timestamp = dts.getMillis();
        // 时区转换，转换成东八区
        DateTime dateTime = dts.toDateTime(DateTimeZone.forTimeZone(TimeZone.getTimeZone("GMT+8")));
        year = dateTime.getYear();
        month = dateTime.getMonthOfYear();
        day = dateTime.getDayOfMonth();
        hour = dateTime.getHourOfDay();
        minute = dateTime.getMinuteOfHour();
        // 组装真实数据
        StringBuilder sb = new StringBuilder();
        sb.append(timestamp).append(COLUMN_SPLIT)
                .append(year).append(COLUMN_SPLIT)
                .append(month).append(COLUMN_SPLIT)
                .append(day).append(COLUMN_SPLIT)
                .append(hour).append(COLUMN_SPLIT)
                .append(minute).append(COLUMN_SPLIT);
        int size = dataMap.size();
        int i = 0;
        for (Map.Entry<String, Object> entry : dataMap.entrySet()) {
            Object val = entry.getValue();
            String strVal = null == val ? HIVE_NULL_VALUE : val.toString();
            sb.append(strVal);
            if (i == (size - 1)) {
                break;
            }
            sb.append(COLUMN_SPLIT);
            i++;
        }
        dataText = sb.toString();
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getYear() {
        return year;
    }

    public int getMonth() {
        return month;
    }

    public int getDay() {
        return day;
    }

    public int getHour() {
        return hour;
    }

    public int getMinute() {
        return minute;
    }

    public String getDataText() {
        return dataText;
    }

    public String getDataSource() {
        return dataSource;
    }

    public String getInterval() {
        return interval;
    }
}
