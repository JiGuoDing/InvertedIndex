package nju.jgd.tf_idf;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TF_IDFMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    private String word = new String();
    private List<String> docs_word =  new ArrayList<String>();
    private List<Integer> freqs = new ArrayList<Integer>();
    private HashSet<String> docSet = new HashSet<String>();
    private double IDF = 0;

    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        /*
          前一个MR的输出形如 "[word] \tab avg_freq, 第一部-xxx:3;......"
          输入的键是 "<[word]>"，值是"avg_freq, 第一部-xxx:3;......>"
         */
        Pattern word_pattern = Pattern.compile("(\\[.*?\\])");
        Matcher word_matcher = word_pattern.matcher(value.toString());
        if (word_matcher.find()) {
            // 提取 [word]
            try {
                word = word_matcher.group(1);
            } catch (NumberFormatException e) {
                e.printStackTrace();
            }
        }

        Pattern doc_pattern = Pattern.compile(",([^,]+):([0-9]+)");
        Matcher doc_matcher = doc_pattern.matcher(value.toString());
        while (doc_matcher.find()) {
            // 提取 文档名 和 对应频数
            try {
                /*
                    输出的键是 "第一部-xxx, [word]"， 输出的值为 "3, 出现 [word] 的文档数"
                 */
                docSet.add(doc_matcher.group(1));
                docs_word.add(word + ", " + doc_matcher.group(1));
                freqs.add(Integer.parseInt(doc_matcher.group(2)));
            } catch (NumberFormatException e) {
                e.printStackTrace();
            }
        }

        IDF = Math.log((double) 7/(docSet.size()+1));

        for (int i = 0; i < docs_word.size(); i++) {
            Text doc_word = new Text(docs_word.get(i));
            DoubleWritable tf_idf =  new DoubleWritable(freqs.get(i) * IDF);
            // 输出格式为："文档名, 单词"  TF-IDF值
            context.write(doc_word, tf_idf);
        }
    }
}
