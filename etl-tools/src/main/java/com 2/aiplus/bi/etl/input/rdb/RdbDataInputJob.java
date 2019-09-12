package com.aiplus.bi.etl.input.rdb;


import com.aiplus.bi.etl.IdGenerator;
import com.aiplus.bi.etl.input.DataInputJobConfiguration;
import com.aiplus.bi.etl.input.DataInputs;
import com.aiplus.bi.etl.input.MapReduceJob;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @author dev
 */
public class RdbDataInputJob extends MapReduceJob {

    public RdbDataInputJob(String jobId) {
        super(jobId);
    }

    public RdbDataInputJob(String jobId, DataInputJobConfiguration jobConfiguration) {
        super(jobId, jobConfiguration);
    }

    public static void main(String[] args) throws Exception {
        int exit = ToolRunner.run(new RdbDataInputJob(IdGenerator.generateUUID()), args);
        System.exit(exit);
    }

    @Override
    protected void preConfigureHadoopJob(Configuration conf, String[] jobArgs) {
        // 设置Mapper数量
        if (getJobConfiguration().getTaskNum() > 0) {
            conf.setInt(JobContext.NUM_MAPS, getJobConfiguration().getTaskNum());
        } else {
            conf.setInt(JobContext.NUM_MAPS, getJobConfiguration().getMappers().length);
        }
    }

    @Override
    protected void configureHadoopJob(Job job) {

        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        job.setJarByClass(ETLMapper.class);
        job.setMapperClass(ETLMapper.class);
        job.setInputFormatClass(RdbInputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
    }

    @Override
    protected void postConfigureHadoopJob() throws Exception {
        // 判断是否需要新建Hive表
        if (getJobConfiguration().getOutput().getType() == DataInputs.Type.HIVE) {
            // 针对Hive的输出单独进行处理
            new RdbOutputHiveSchemaTransfer(getJobConfiguration(), getJobId()).output();
        }
    }

    public static class ETLMapper extends Mapper<LongWritable, RowWritable, NullWritable, Text> {

        private static final Log MAPPER_LOG = LogFactory.getLog(ETLMapper.class);

        private static final String COUNTER_GROUP_SOURCE_TABLE = "ETL Source Table";

        private static final String COUNTER_GROUP_TARGET_TABLE = "ETL Target Table";

        private static final int SOURCE_TABLE_GROUP_SALT = 20;

        private static final int DEFAULT_ETL_COUNTER_MAX = 60;
        private MultipleOutputs<NullWritable, Text> mos;
        private int sourceTableNum;

        public ETLMapper() {
            super();
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            mos = new MultipleOutputs(context);
            this.sourceTableNum = context.getConfiguration().getInt(RdbInputFormat.DATA_INPUT_SOURCE_TABLE_NUM_PROPERTY, 0);
            MAPPER_LOG.info("Get source table num is " + sourceTableNum);
        }

        @Override
        protected void map(LongWritable key, RowWritable value, Context context) throws IOException, InterruptedException {
            // source table counter
            context.getCounter(COUNTER_GROUP_SOURCE_TABLE, getCounterSourceTableKeyName(value.getTableName(), value.getTargetTable())).increment(1);
            // target table counter
            context.getCounter(COUNTER_GROUP_TARGET_TABLE, value.getTargetTable()).increment(1);
            // write multi output.
            mos.write(NullWritable.get(), new Text(value.getDataText()), generateFileName(value));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            mos.close();
        }

        private String generateFileName(RowWritable value) {
            return value.getTargetTable() + "/" + value.getTableName();
        }

        private String getCounterSourceTableKeyName(String sourceTable, String targetTable) {
            if (sourceTableNum > DEFAULT_ETL_COUNTER_MAX) {
                return targetTable + "-" + (Math.abs(sourceTable.hashCode()) % SOURCE_TABLE_GROUP_SALT);
            }
            return sourceTable;
        }
    }
}
