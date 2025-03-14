package nju.jgd.sortIndex;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortIndexReducer extends Reducer<FloatWritable, Text, Text, Text> {
    @Override
    protected void reduce(FloatWritable key, Iterable<Text> values, Reducer<FloatWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        /**
         * 输入的键是词频
         * 输入的值为单词-词频信息 "[word],第一部-xxx:3" 的列表
         */
        for (Text value : values) {
            String[] split = value.toString().split(",");
            Text word = new Text(split[0].trim());
            String freq_info = split[1].trim();
            // 输出的值为 "avg_freq, 第一部-xxx:3......>"
            Text avg_freq_info = new Text(key.toString() + ", " + freq_info);
            context.write(word, avg_freq_info);
        }
    }
}
