package com.aiplus.bi.etl;

import com.aiplus.bi.etl.input.DataInputJobConfiguration;
import com.aiplus.bi.etl.input.DataInputs;
import com.aiplus.bi.etl.input.rdb.RdbInputFormat;
import com.aiplus.bi.etl.input.rdb.RdbInputSplit;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

public class InputFormatTest {

    private static final String ETL_JOB_CONFIG_FILE = "etl_job_call_detail_bill_all.json";

    private RdbInputFormat inputFormat;

    private Configuration configuration = new Configuration();

    private DataInputJobConfiguration jobConfiguration;

    private DataInputs dataInputs;

    private String etlJobId;

    @Before
    public void initInputFormat() throws IOException {

        String json = loadJobConfiguration();

        this.jobConfiguration = new GsonBuilder().serializeNulls().create().fromJson(json, DataInputJobConfiguration.class);
        this.etlJobId = "c9acc1b0-1693-4180-97d9-ea20eb72516";
        DataInputs.configureDataInputJob(configuration, etlJobId, "etl_job_win_cdr_trunk2017", json);
        // 设置Mapper数量
        if (jobConfiguration.getTaskNum() > 0) {
            configuration.setInt(JobContext.NUM_MAPS, jobConfiguration.getTaskNum());
        } else {
            configuration.setInt(JobContext.NUM_MAPS, jobConfiguration.getMappers().length);
        }
        inputFormat = new RdbInputFormat();
        inputFormat.setConf(configuration);
        this.dataInputs = inputFormat.getDataInputs();
        System.out.println("Task num: " + jobConfiguration.getTaskNum());
    }

    private String loadJobConfiguration() throws IOException {
        try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(ETL_JOB_CONFIG_FILE)) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String line;
            StringBuilder json = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                json.append(line);
            }
            return json.toString();
        }
    }

    @Test
    public void testInputSplit() {
        try {
            List<InputSplit> splits = inputFormat.getSplits(null);
            for (InputSplit split : splits) {
                RdbInputSplit rdbInputSplit = (RdbInputSplit) split;
                System.out.println(rdbInputSplit.getTable() + "\t" + rdbInputSplit.getLength() + "\t" + rdbInputSplit.getStart() + "," + rdbInputSplit.getEnd());
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
