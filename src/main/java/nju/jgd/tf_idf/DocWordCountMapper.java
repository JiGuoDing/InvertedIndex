package nju.jgd.tf_idf;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DocWordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text docNameKey = new Text();
    private IntWritable wordCountValue = new IntWritable();
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        // 解析格式: "[word]    avg_freq, 文档A:3;文档B:4;文档C:5"
        Pattern pattern = Pattern.compile("\\[(.*?)\\]\\s+(\\d+\\.\\d+),\\s*(.*)");
        Matcher matcher = pattern.matcher(value.toString());

        if (matcher.find()) {
            String freq_info = matcher.group(3);  // "文档A:3;文档B:4;文档C:5"

            // 解析每个 "文档名:词数"
            String[] docs = freq_info.split(";");
            for (String docEntry : docs) {
                String[] parts = docEntry.split(":");
                if (parts.length == 2) {
                    String docName = parts[0].trim();
                    int wordCount = Integer.parseInt(parts[1].trim());

                    // 以 文档名 作为 key
                    docNameKey.set(docName);
                    wordCountValue.set(wordCount);
                    // 输出格式："文档名    词数"
                    context.write(docNameKey, wordCountValue);
                }
            }
        }
    }
}
