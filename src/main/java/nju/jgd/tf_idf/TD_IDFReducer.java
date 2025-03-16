package nju.jgd.tf_idf;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TD_IDFReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        /*
            输入的键是 "第一部-xxx, [word]"， 输入的值的元素为 "3, 出现 [word] 的文档数"
         */

    }
}
