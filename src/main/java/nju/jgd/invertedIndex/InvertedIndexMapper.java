package nju.jgd.invertedIndex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final Text KeyInfo = new Text();
    // private static final Text ValueInfo = new Text("1");
    private static final Text one = new Text("1");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String[] fields = StringUtils.split(line, ' ');

        // 用于获取该context的数据分片信息，如起始位置、长度等
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        // 拿到file文档的名字
        String fileName = fileSplit.getPath().getName();
        // key写成<单词：文档：    1>的形式
        for (String field: fields) {
            field = '[' + field + ']';
            KeyInfo.set(field + ":" + fileName);
            context.write(KeyInfo, one);
        }
    }
}

/**
 * InputSplit表示输入数据的一个逻辑切片，每个文件按128MB划分为多个InputSplit。
 * public class FileSplit extends InputSplit implements Writable {
 *     private Path file;         // 文件路径
 *     private long start;        // 分片起始位置
 *     private long length;       // 分片长度
 * }
 * 每个Mapper处理一个FileSplit
 */
