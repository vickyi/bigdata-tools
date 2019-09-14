package com.aiplus.bi.etl.input.druid;

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
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author dev
 */
public class DruidDataInputJob extends MapReduceJob implements DruidInputConfigurable {

    private static final Log LOG = LogFactory.getLog(DruidDataInputJob.class);

    private String strExportDate;

    public DruidDataInputJob(String jobId) {
        super(jobId);
    }

    public DruidDataInputJob(String jobId, DataInputJobConfiguration jobConfiguration) {
        super(jobId, jobConfiguration);
    }

    public static void main(String[] args) throws Exception {
        int exit = ToolRunner.run(new DruidDataInputJob(IdGenerator.generateUUID()), args);
        System.exit(exit);
    }

    @Override
    protected void preConfigureHadoopJob(Configuration conf, String[] jobArgs) throws Exception {
        if (null == jobArgs || 0 == jobArgs.length) {
            throw new IllegalArgumentException("Job args not found!");
        }
        LOG.info("Druid data input job args: " + Arrays.toString(jobArgs));
        conf.set(DRUID_JOB_ARGS_EXPORT_DATE, jobArgs[0]);
        this.strExportDate = jobArgs[0];
    }

    @Override
    protected void configureHadoopJob(Job job) throws Exception {
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        job.setJarByClass(DruidDataMapper.class);
        job.setInputFormatClass(SegmentInputFormat.class);
        job.setMapperClass(DruidDataMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(EventWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
    }

    @Override
    protected void postConfigureHadoopJob() throws Exception {
        // 判断是否需要新建Hive表
        if (getJobConfiguration().getOutput().getType() == DataInputs.Type.HIVE) {
            // 针对Hive的输出单独进行处理
            new DruidOutputHiveSchemaTransfer(getJobConfiguration(), getJobId(), strExportDate).output();
        }
    }

    public static class DruidDataMapper extends Mapper<LongWritable, EventWritable, NullWritable, Text> {

        private static final String COUNTER_GROUP_DATASOURCE = "Druid DataSource";

        private MultipleOutputs<NullWritable, Text> mos;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            mos = new MultipleOutputs(context);
        }

        @Override
        protected void map(LongWritable key, EventWritable value, Context context) throws IOException, InterruptedException {
            String yyyyMMdd = value.getYear() + "-" + value.getMonth() + "-" + value.getDay();
            String yyyyMMddHr = yyyyMMdd + "-" + value.getHour() + "hr";
            String yyyyMMddTotal = yyyyMMdd + "-total";
            context.getCounter(COUNTER_GROUP_DATASOURCE + " " + value.getDataSource(), yyyyMMddHr).increment(1);
            context.getCounter(COUNTER_GROUP_DATASOURCE + " " + value.getDataSource(), yyyyMMddTotal).increment(1);
            mos.write(NullWritable.get(), new Text(value.getDataText()), value.getDataSource() + "/" + yyyyMMddHr);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            mos.close();
        }
    }
}
