package nju.jgd.invertedIndex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text> {
    private static Text info = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 单词和文档名相同的，value数目先聚合一下
        int sum = 0;
        for (Text value : values) {
            sum += Integer.parseInt(value.toString());
        }
        // 为了使相同单词映射到同一个ReducerTask中，把文件名拆分至value中，写新的键值对<单词：   文件名：数目>
        int splitIndex = key.toString().indexOf(':');
        // 设置输出值为"文件名:数目"
        info.set(key.toString().substring(splitIndex+1) + ':' + sum);
        // 设置输出键为"单词"
        key.set(key.toString().substring(0, splitIndex));
        context.write(key, info);
    }
}
