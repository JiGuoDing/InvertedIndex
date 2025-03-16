package nju.jgd.tf_idf;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TF_IDFMapper extends Mapper<Object, Text, Text, Text> {
    private String word = new String();
    private List<String> docs_word =  new ArrayList<String>();
    private List<String> freq_related_docs_num = new ArrayList<String>();
    private HashSet<String> docSet = new HashSet<String>();

    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
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
                freq_related_docs_num.add(doc_matcher.group(2));
            } catch (NumberFormatException e) {
                e.printStackTrace();
            }
        }

        for (int i = 0; i < freq_related_docs_num.size(); i++) {
            freq_related_docs_num.set(i, freq_related_docs_num.get(i) + ", " + docSet.size());
            Text doc_word = new Text(docs_word.get(i));
            Text freq_doc_num = new Text(freq_related_docs_num.get(i));
            context.write(doc_word, freq_doc_num);
        }
    }
}
