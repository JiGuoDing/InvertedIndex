package nju.jgd.invertedIndex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
    private static Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String fileList = new String();

        for (Text value : values) {
            fileList += (value.toString() + ";");
        }

        result.set(fileList);
        context.write(key, result);
    }
}
