package nju.jgd.tf_idf;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DocWordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable totalWordCount = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        totalWordCount.set(sum);
        // 输出格式："文档名    总词数"
        context.write(key, totalWordCount);
    }
}
