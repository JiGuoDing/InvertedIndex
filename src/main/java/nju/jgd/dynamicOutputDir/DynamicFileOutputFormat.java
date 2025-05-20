package nju.jgd.dynamicOutputDir;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * DynamicFileOutputFormat用于在Reducer中将结果输出到多个目录中
 */
public class DynamicFileOutputFormat extends FileOutputFormat<Text, Text> {
    @Override
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        // 获取Job的配置和输出目录
        Configuration conf =  taskAttemptContext.getConfiguration();
        Path outputDir = getOutputPath(taskAttemptContext);

        // 创建自定义的RecordWriter
        return new DynamicRecordWriter(conf, outputDir);
    }
}
