package nju.jgd.test;

public class test {
    public static void main(String[] args) {
        String str = "[一下],第一部-xxx:3";
        String[] split = str.split(",");
        for (String s : split) {
            System.out.println(s);
        }
        System.out.println(str.split(",").length);
    }
}
