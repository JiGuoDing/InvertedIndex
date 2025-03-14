package nju.jgd.sortIndex;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SortIndexMapper extends Mapper<LongWritable, Text, FloatWritable, Text> {

    private Text word_freq_info = new Text();
    private FloatWritable avg_freq = new FloatWritable();
    // private static final Logger logger = Logger.getLogger(SortIndexMapper.class.getName());
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FloatWritable, Text>.Context context) throws IOException, InterruptedException {
        /*
          按平均词频对单词进行降序排列
          前一个MR的输出形如 "[word] \tab avg_freq, 第一部-xxx:3......"
          输入的键是 "<[word]>"，值是"avg_freq, 第一部-xxx:3......>"
        /*
          使用正则表达式提取出avg_freq <\\s*(\d+\.\d+),>
          \\s*：匹配任意数量的空白字符；
          (\d+\.\d+)：匹配一个或多个数字，后跟一个点.，再跟一个或多个数字，并捕获到分组中；
          ,：匹配逗号作为结束符号。
         */
        // logger.setLevel(java.util.logging.Level.INFO);
        Pattern pattern = Pattern.compile("\\s*(\\d+\\.\\d+),");
        Matcher matcher = pattern.matcher(value.toString());
        if (matcher.find()) {
            // 提取第一个捕获组，并为输出键设置值
            try {
                avg_freq.set(Float.parseFloat(matcher.group(1)));
            } catch (NumberFormatException e) {
                System.err.println("Invalid avg_freq: " + matcher.group(1));
            }
        }
        /*
          使用正则表达式提取出[word]和单词文档频数信息 <(\\[.*?\\])|([^,]+:\\d+)>
          pattern = Pattern.compile("(\\[.*?\\])|([^,]+:\\d+)");
        */

        String[] split = value.toString().split("[\t,]");

        String word = split[0].trim();
        String freq_info = split[2].trim();
        word_freq_info.set(word + "," + freq_info);

        /**
         * 输出的键是词频
         * 输出的值为单词-词频信息 "[word],第一部-xxx:3"
         */
        context.write(avg_freq, word_freq_info);
    }
}
