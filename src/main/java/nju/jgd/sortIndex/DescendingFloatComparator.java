package nju.jgd.sortIndex;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DescendingFloatComparator extends WritableComparator {

    /**
     * 坑点记录<br/>
     * 参数：<br/>
     *  - map的输出的键的类型<br/>
     *  - 是否开启自定义比较<br/>
     * 没有这两个参数就会一直报错 NullPointerException
     */
    public DescendingFloatComparator(){
        super(FloatWritable.class, true);
    }

    // 降序排列
    // @Override
    // public int compare(Object a, Object b) {
    //     FloatWritable f1 = (FloatWritable) a;
    //     FloatWritable f2 = (FloatWritable) b;
    //
    //     // 注意f1和f2的顺序，为了实现降序排列
    //     return Float.compare(f2.get(), f1.get());
    // }

    // 降序排列
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        FloatWritable f1 = (FloatWritable) a;
        FloatWritable f2 = (FloatWritable) b;
        return f2.compareTo(f1);
    }
}
