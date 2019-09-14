package com.aiplus.bi.etl.input;

import com.google.gson.GsonBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * 数据摄入的MapReduce类型的Job的抽象类，所有的MapReduce类型的Job都需要继承这个类.
 *
 * @author dev
 */
public abstract class MapReduceJob extends Configured implements Tool {

    private static final Log LOG = LogFactory.getLog(MapReduceJob.class);

    private String jobId;

    private DataInputJobConfiguration jobConfiguration;

    public MapReduceJob(String jobId) {
        this(jobId, null);
    }

    public MapReduceJob(String jobId, DataInputJobConfiguration jobConfiguration) {
        this.jobId = jobId;
        this.jobConfiguration = jobConfiguration;
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String etlJobConfigurationJson;
        if (null == jobConfiguration) {
            String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            if (remainingArgs.length < 1) {
                throw new IllegalArgumentException("Not found data input job configuration file path.");
            }
            etlJobConfigurationJson = readDataInputJobConfigurationFile(remainingArgs[0]);
            jobConfiguration = new GsonBuilder().serializeNulls().create().fromJson(etlJobConfigurationJson, DataInputJobConfiguration.class);
        } else {
            etlJobConfigurationJson = new GsonBuilder().serializeNulls().create().toJson(getJobConfiguration());
        }

        // 保存配置信息
        DataInputs.configureDataInputJob(conf, getJobId(), jobConfiguration.getJobName(), etlJobConfigurationJson);

        // 每次进行ETL任务的时候都需要产生一个唯一标识，并以这个唯一标识作为文件夹，输出结果都放在指定的标识下
        String output = this.jobConfiguration.getOutput().getPath() + "/" + this.jobId;
        Path outputPath = new Path(output);
        FileSystem fs = FileSystem.get(conf);
        if (!fs.exists(outputPath)) {
            // 如果输出文件夹不存在，则创建一个新的输出文件夹
            fs.makeQualified(outputPath);
        }
        LOG.info("Data output: " + output);

        // 配置Job之前的操作
        preConfigureHadoopJob(conf, args);

        Job job = Job.getInstance(conf);
        configureOutput(job, outputPath);
        job.setJobName("ETL-Job(" + jobConfiguration.getJobName() + "[" + output + "])");
        // 配置Job
        configureHadoopJob(job);
        // 运行Job
        int jobExitCode = job.waitForCompletion(true) ? 0 : 1;

        // Job运行完成以后的操作
        postConfigureHadoopJob();

        return jobExitCode;
    }

    protected abstract void preConfigureHadoopJob(Configuration conf, String[] jobArgs) throws Exception;

    protected abstract void configureHadoopJob(Job job) throws Exception;

    protected abstract void postConfigureHadoopJob() throws Exception;

    protected void configureOutput(Job job, Path output) throws Exception {
        FileOutputFormat.setOutputPath(job, output);
    }

    private String readDataInputJobConfigurationFile(String dataInputJobConfigFile) throws IOException {
        // 获取Job的配置文件路径
        try (FileReader fr = new FileReader(dataInputJobConfigFile)) {
            BufferedReader reader = new BufferedReader(fr);
            String line;
            StringBuilder json = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                json.append(line);
            }
            return json.toString();
        } catch (FileNotFoundException e) {
            throw new IOException(e);
        }
    }

    public String getJobId() {
        return jobId;
    }

    public DataInputJobConfiguration getJobConfiguration() {
        return this.jobConfiguration;
    }
}
