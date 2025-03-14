package nju.jgd.test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class test {
    public static void main(String[] args) {
        String str = "[一下]\t1.33, cn_stopwords.txt:1; ";
        Pattern pattern = Pattern.compile("\\s*(\\d+\\.\\d+),");
        Matcher matcher = pattern.matcher(str);
        if(matcher.find()){
            System.out.println(matcher.group(1));
        }
    }
}
