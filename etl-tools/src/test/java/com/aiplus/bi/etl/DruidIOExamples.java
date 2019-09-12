package com.aiplus.bi.etl;

import com.fasterxml.jackson.databind.InjectableValues;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Yielder;
import io.druid.java.util.common.guava.Yielders;
import io.druid.math.expr.ExprMacroTable;
import io.druid.query.Druids;
import io.druid.query.Result;
import io.druid.query.select.*;
import io.druid.segment.IndexIO;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexIndexableAdapter;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.*;

public class DruidIOExamples {

    private static final String DATA_DIR_PATH = "DATA_DIR_PATH";

    private static final String DETAIL_PATH = "DETAIL_PATH";

    private static final String DATA_TEST_OUT = "DATA_TEST_OUT_PATH";

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");

    public static void main(String[] args) throws IOException {
        long start = System.currentTimeMillis();
        final InjectableValues.Std injectableValues = new InjectableValues.Std();
        injectableValues.addValue(ExprMacroTable.class, ExprMacroTable.nil());

        IndexIO indexIO = new IndexIO(
                new DefaultObjectMapper().setInjectableValues(injectableValues),
                OffHeapMemorySegmentWriteOutMediumFactory.instance(),
                () -> 0
        );

        File dataTestOut = new File(DATA_TEST_OUT);
        if (!dataTestOut.delete()) {
            System.out.println("Warning!!! Delete [" + DATA_TEST_OUT + "] failed!");
        }

        FileWriter fw = new FileWriter(dataTestOut, true);
        PrintWriter out = new PrintWriter(fw);

        QueryableIndex indexData = indexIO.loadIndex(new File(DETAIL_PATH));
        QueryableIndexIndexableAdapter index = new QueryableIndexIndexableAdapter(indexData);
        int rowLength = index.getNumRows();
        out.println(sdf.format(new Date()));
        out.println(index);
        out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~1~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        out.println("INTERVAL: " + index.getDataInterval());
        out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~2~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        out.println("DIM SIZE: " + index.getDimensionNames().size());
        out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~3~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        List<String> dimensionNames = new ArrayList<>();
        for (String availableDimension : index.getDimensionNames()) {
            out.println("DIM NAME: " + availableDimension);
            dimensionNames.add(availableDimension);
        }
        out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~4~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        out.println("COL SIZE: " + index.getMetricNames().size());
        out.println("ROW SIZE: " + rowLength);
        out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~5~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        List<String> metricNames = new ArrayList<>();
        for (String columnName : index.getMetricNames()) {
            out.println("COL NAME: " + columnName);
            metricNames.add(columnName);
        }
        out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~6~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

        SelectQueryEngine selectQueryEngine = new SelectQueryEngine();
        Druids.SelectQueryBuilder selectQueryBuilder = new Druids.SelectQueryBuilder();
        SelectQuery selectQuery = selectQueryBuilder.dataSource("ai_plus_prod")
                .intervals(index.getDataInterval().toString())
                .dimensions(dimensionNames)
                .metrics(metricNames)
                .pagingSpec(PagingSpec.newSpec(index.getNumRows()))
                .build();
        Sequence<Result<SelectResultValue>> resultSequence = selectQueryEngine.process(selectQuery, new QueryableIndexSegment("aaa", indexData));
        Yielder<Result<SelectResultValue>> yielder = Yielders.each(resultSequence);
        Result<SelectResultValue> result = yielder.get();
        SelectResultValue resultValue = result.getValue();
        for (EventHolder eventHolder : resultValue.getEvents()) {
            StringBuilder sb = new StringBuilder();
            Map<String, Object> event = eventHolder.getEvent();
            DateTime dateTime = (DateTime) event.get("timestamp");
            long utcTimestamp = dateTime.getMillis();
            dateTime = dateTime.toDateTime(DateTimeZone.forTimeZone(TimeZone.getTimeZone("GMT+8")));
            long gmt8Timestamp = dateTime.getMillis();
            sb.append(utcTimestamp).append("/").append(gmt8Timestamp).append("/").append(dateTime).append("/").append(dateTime.getYear()).append("-").append(dateTime.getMonthOfYear()).append("-").append(dateTime.getDayOfMonth())
                    .append(" ")
                    .append(dateTime.getHourOfDay()).append(":").append(dateTime.getMinuteOfHour()).append(":").append(dateTime.getSecondOfMinute());
            for (Map.Entry<String, Object> entry : event.entrySet()) {
                if (entry.getKey().equalsIgnoreCase("timestamp")) {
                    continue;
                }
                sb.append(entry.getValue()).append("\t");
            }
            out.println(sb.toString());
        }
        out.flush();
        out.close();
        fw.close();
        System.out.println("Extract all data use: " + (System.currentTimeMillis() - start) + "ms.");
    }
}
