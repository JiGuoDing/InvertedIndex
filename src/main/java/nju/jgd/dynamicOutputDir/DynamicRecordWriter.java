package nju.jgd.dynamicOutputDir;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DynamicRecordWriter extends RecordWriter<Text, Text> {

    private final FileSystem fs;
    private final Path baseOutputDir;
    private final Configuration conf;
    private final Map<String, RecordWriter<Text, Text>> writers = new HashMap<>();

    public Path getBaseOutputDir() {
        return baseOutputDir;
    }

    public Configuration getConf() {
        return conf;
    }

    public FileSystem getFs() {
        return fs;
    }

    public DynamicRecordWriter(Configuration conf, Path baseOutputDir) throws IOException {
        this.conf = conf;
        this.baseOutputDir = baseOutputDir;
        this.fs = baseOutputDir.getFileSystem(conf);
    }


    @Override
    public void write(Text key, Text value) throws IOException, InterruptedException {
        // 动态生成子目录（按key的首字母分组）
        String subDir = "data_" + key.toString().charAt(0);
        Path outputPath = new Path(baseOutputDir, subDir);

        // 如果该目录的RecordWriter不存在，则创建
        if(!writers.containsKey(subDir)) {
            // 使用默认的TextOutputFormat创建RecordWriter
            TextOutputFormat<Text, Text> outputFormat =  new TextOutputFormat<>();
            RecordWriter<Text, Text> writer = outputFormat.getRecordWriter(
                    new TaskAttemptContextImpl(conf, new TaskAttemptID()));
            writers.put(subDir, writer);
        }

        // 写入数据
        writers.get(subDir).write(key, value);
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        // 关闭所有 RecordWriter
        writers.forEach((s, writer) -> {
            try {
                writer.close(taskAttemptContext);
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
