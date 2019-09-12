package com.aiplus.bi.etl.input.rdb;

import com.aiplus.bi.etl.input.DataInputJobConfiguration;
import com.aiplus.bi.etl.input.DataInputs;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * RDB的输入格式化，分片规则定义.
 *
 * @author dev
 */
public class RdbInputFormat extends InputFormat<LongWritable, RowWritable> implements Configurable {

    public static final String DATA_INPUT_SOURCE_TABLE_NUM_PROPERTY = "etl.source.table.num";
    private static final Log LOG = LogFactory.getLog(RdbInputFormat.class);
    private static final int NEED_USE_MULTI_THREAD_TABLE_NUM = 3;
    private DataInputs dataInputs;

    private DataInputJobConfiguration jobConfiguration;

    public DataInputs getDataInputs() {
        return dataInputs;
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        try (Connection connection = this.dataInputs.getSourceConnection()) {
            DatabaseMetaData dmd = connection.getMetaData();
            // 1. 拿到所有的表的映射
            List<Table> tables = fetchTables(dmd);
            // 设置表的数量
            getConf().setInt(DATA_INPUT_SOURCE_TABLE_NUM_PROPERTY, tables.size());
            LOG.info("SET ETL source table num is " + tables.size());
            // 总的记录集大小
            long totalRecordCount;
            long start = System.currentTimeMillis();
            // 2. 取出表的所有字段，这个地方为了处理多表的情况，使用多线程的方法去取
            if (tables.size() > NEED_USE_MULTI_THREAD_TABLE_NUM) {
                // 大于3个的时候就需要使用到线程池去处理了，这里开4个线程就好，太多了对数据库连接有影响
                ExecutorService executorService = Executors.newFixedThreadPool(4);
                ExecutorCompletionService<Table> completionService = new ExecutorCompletionService<>(executorService);
                try {
                    // 提交
                    for (Table table : tables) {
                        completionService.submit(new GetSourceTableColumnsCallable(table));
                    }

                    // 拿结果
                    int taskNum = tables.size();
                    tables.clear();
                    long tc = 0L;
                    while (taskNum-- > 0) {
                        Future<Table> future = completionService.take();
                        Table t = future.get();
                        LOG.info("Getting table[" + taskNum + "] metadata: " + t.getName() + "\tcount: " + t.getLength());
                        tables.add(t);
                        tc = tc + t.getLength();
                    }
                    totalRecordCount = tc;
                } catch (ExecutionException e) {
                    throw new IOException(e);
                } finally {
                    // 强制最后必须关系线程池来释放资源
                    executorService.shutdown();
                }
            } else {
                long tc = 0L;
                // 非多线程的情况下只用直接去取就行了
                for (int j = 0; j < tables.size(); j++) {
                    Table table = tables.get(j);
                    TableHelper.getSourceTableColumns(dmd, table);
                    TableHelper.getTableSplit(connection, table);
                    tc = tc + table.getLength();
                    LOG.info("Getting table[" + (j + 1) + "] metadata: " + table.getName() + "\tcount: " + table.getLength());
                }
                totalRecordCount = tc;
            }
            LOG.info("Loaded table(s) info use " + (System.currentTimeMillis() - start) + "ms. Total record count is " + totalRecordCount);
            // 3. 分片处理
            start = System.currentTimeMillis();
            List<List<RdbInputSplit>> splitGroup = table2SplitGroup(tables, totalRecordCount);
            LOG.info("Split table to group use " + (System.currentTimeMillis() - start) + "ms. Total group size is " + splitGroup.size());
            return generateInputSplit(splitGroup);
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    private List<InputSplit> generateInputSplit(List<List<RdbInputSplit>> splitGroup) {
        List<InputSplit> splits = new ArrayList<>();
        for (List<RdbInputSplit> splitList : splitGroup) {
            for (int i = 0; i < splitList.size() - 1; i++) {
                splitList.get(i).setNextSplit(splitList.get(i + 1));
            }
            splits.add(splitList.get(0));
        }
        return splits;
    }

    private List<Table> fetchTables(DatabaseMetaData dmd) throws SQLException {
        List<Table> tables = new ArrayList<>();
        DataInputJobConfiguration.TableMapper[] tableMappers = jobConfiguration.getMappers();
        // 将所有的表取出来
        for (DataInputJobConfiguration.TableMapper tableMapper : tableMappers) {
            String targetTable = tableMapper.getTargetTable();
            String[] fields = null;
            String sourceTable;
            if (null != tableMapper.getSourceTable() && !"".equalsIgnoreCase(tableMapper.getSourceTable())) {
                // 直接配置的源表名称
                sourceTable = tableMapper.getSourceTable();
            } else {
                // 没有配置源表名称，则认为是配置的源表名称加上源表需要同步的字段
                sourceTable = tableMapper.getSource().getTableName();
                fields = tableMapper.getSource().getFields();
            }
            // 匹配表名称
            if (sourceTable.contains("%")) {
                // 只有有通配符的表需要查询取出来
                try (ResultSet rs = dmd.getTables(null, "%", sourceTable, new String[]{"TABLE"})) {
                    while (rs.next()) {
                        String tableName = rs.getString("TABLE_NAME");
                        tables.add(new Table(tableName, targetTable, tableMapper.getSplitKey(), new ColumnList(fields)));
                    }
                }
            } else {
                tables.add(new Table(sourceTable, targetTable, tableMapper.getSplitKey(), new ColumnList(fields)));
            }
        }
        return tables;
    }

    private List<List<RdbInputSplit>> table2SplitGroup(List<Table> tables, long totalRecordCount) {
        // 获取Map Task的数量
        int numOfMapTask = getConf().getInt(JobContext.NUM_MAPS, 1);
        LOG.info("Get map task num is " + numOfMapTask);
        // 计算均分步长
        int stepRecordCount = (int) (totalRecordCount / numOfMapTask);
        LOG.info("Generate step record count is " + stepRecordCount);
        // 对table进行全局排序，按照记录数从小到大进行排序
        tables.sort((o1, o2) -> -(int) (o1.getLength() - o2.getLength()));
        long currentLastCount = 0L;
        List<List<RdbInputSplit>> splitGroup = new ArrayList<>(numOfMapTask);
        List<RdbInputSplit> partOfSplits = new ArrayList<>();
        List<RdbInputSplit> lastPartOfSplits = new ArrayList<>();
        List<Table> litterRecordList = new ArrayList<>();
        for (Table table : tables) {
            if (table.getLength() == 0) {
                continue;
            }
            // 如果数量正好和分片处理数量相等，则单独为一组
            if (table.getLength() == stepRecordCount) {
                partOfSplits.add(RdbInputSplit.createFromTable(table));
                // 当达到当前的组的峰值，则不将后续的添加到当前组
                splitGroup.add(new ArrayList<>(partOfSplits));
                // 需要将集合清空，方便下次进行新的缓存
                partOfSplits.clear();
                // 不进行下面的业务处理
                continue;
            }
            // 如果数量大于分片处理数量，则需要进行数据切分
            if (table.getLength() > stepRecordCount) {
                int loop = (int) (table.getLength() / stepRecordCount);
                if (table.getLength() % stepRecordCount > 0) {
                    loop = loop + 1;
                }
                for (int i = 0; i < loop; i++) {
                    long start = table.getStart() + stepRecordCount * i - 1;
                    if (start < 0L) {
                        // 开始不能够为0
                        start = 0L;
                    }
                    long end = start + stepRecordCount;
                    if (end > table.getEnd()) {
                        // 此处表示该切分已经不满足一个分片的处理数量了
                        end = table.getEnd();
                        currentLastCount = currentLastCount + (end - start);
                        lastPartOfSplits.add(RdbInputSplit.createFromTableSplit(table, start, end));
                        if (currentLastCount >= stepRecordCount) {
                            splitGroup.add(new ArrayList<>(lastPartOfSplits));
                            lastPartOfSplits.clear();
                            currentLastCount = 0L;
                        }
                        // 这里表示已经是最后一个切片都处理完成了，则不需要进行下面的操作
                        break;
                    }
                    // 将切分的分片加入到分组中
                    partOfSplits.add(RdbInputSplit.createFromTableSplit(table, start, end));
                    // 当达到当前的组的峰值，则不将后续的添加到当前组
                    splitGroup.add(new ArrayList<>(partOfSplits));
                    // 需要将集合清空，方便下次进行新的缓存
                    partOfSplits.clear();
                }
                // 不进行下面的业务处理
                continue;
            }
            litterRecordList.add(table);
        }
        // 最后处理小于分片处理数量的记录
        // 将最后剩下的这些分片拷贝到part集合中，做常规处理
        long currentStepCount = 0L;
        if (!lastPartOfSplits.isEmpty()) {
            partOfSplits.addAll(lastPartOfSplits);
            currentStepCount = currentLastCount;
            // 清空last集合
            lastPartOfSplits.clear();
        }
        // 循环小的进行分组合并
        for (Table table : litterRecordList) {
            partOfSplits.add(RdbInputSplit.createFromTable(table));
            currentStepCount = currentStepCount + table.getLength();
            if (currentStepCount >= stepRecordCount) {
                splitGroup.add(new ArrayList<>(partOfSplits));
                partOfSplits.clear();
                currentStepCount = 0L;
            }
        }
        if (!partOfSplits.isEmpty()) {
            // 如果还有剩下的，那要看map的数量有没有达标，如果达标了，则将剩下的都加入到最后一个分组中，否则起一个新的分组
            if (splitGroup.size() >= numOfMapTask) {
                splitGroup.get(splitGroup.size() - 1).addAll(partOfSplits);
            } else {
                splitGroup.add(new ArrayList<>(partOfSplits));
                partOfSplits.clear();
            }
        }
        return splitGroup;
    }

    @Override
    public RecordReader<LongWritable, RowWritable> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new RowRecordReader(context.getConfiguration(), (RdbInputSplit) split, createConnection());
    }

    private Connection createConnection() {
        try {
            return this.dataInputs.getSourceConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Configuration getConf() {
        return this.dataInputs.getMapReduceConfiguration();
    }

    @Override
    public void setConf(Configuration conf) {
        this.dataInputs = new DataInputs(conf);
        this.jobConfiguration = dataInputs.getJobConfiguration();
    }

    private class GetSourceTableColumnsCallable implements Callable<Table> {

        private Table table;

        private GetSourceTableColumnsCallable(Table table) {
            this.table = table;
        }

        @Override
        public Table call() throws Exception {
            try (Connection connection = dataInputs.getSourceConnection()) {
                DatabaseMetaData dmd = connection.getMetaData();
                // 获取源表的所有列信息
                TableHelper.getSourceTableColumns(dmd, table);
                // 获取表的拆分情况
                TableHelper.getTableSplit(connection, table);
            }
            return table;
        }
    }
}
