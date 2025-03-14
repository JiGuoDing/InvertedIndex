package nju.jgd.invertedIndex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
    private static Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 最终输出的形如 "[word] \tab avg_freq, 第一部-xxx:3......" 的字符串
        String freq_list = new String();
        // 出现该单词的文档数
        Integer file_cnt = 0;
        // 该单词在文档中的平均出频数
        float avg_freq = 0;
        // 该单词在所有文档中出现的总频数
        Integer total_freq = 0;
        // 该单词的 文档:次数 信息
        String fileList = new String();
        // value是一个字符串，形如"第一部-xxx:3"
        for (Text value : values) {
            int index = value.toString().indexOf(':');
            total_freq += Integer.parseInt(value.toString().substring(index + 1).trim());
            file_cnt ++;
            fileList += (value + "; ");
        }

        avg_freq = (float) total_freq / file_cnt;
        freq_list += String.format("%.2f", avg_freq) + ", " + fileList;
        result.set(freq_list);
        context.write(key, result);
    }
}
