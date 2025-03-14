package nju.jgd.sortIndex;

import org.apache.hadoop.io.WritableComparator;

public class DescendingFloatComparator extends WritableComparator {
    // 降序排列
    @Override
    public int compare(Object a, Object b) {
        return -super.compare(a, b);
    }
}
