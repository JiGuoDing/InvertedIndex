package nju.jgd.tf_idf;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class TF_IDFReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    // 简单方法：使用MultipleOutputs类直接实现  多目录输出
    private MultipleOutputs<Text, DoubleWritable> mos;

    @Override
    protected void setup(Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        // 输入格式为："文档名, 单词"  TF-IDF值
        for (DoubleWritable value : values) {
            context.write(key, value);
        }

        // 多目录输出测试
        // String outputDir = "data" + key.toString().charAt(0);
        // mos.write(outputDir, key, new DoubleWritable(0.0));
    }

    @Override
    protected void cleanup(Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        mos.close();
    }
}
