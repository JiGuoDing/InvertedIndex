# 倒排索引
hadoop jar Inverted_Sort_Index-jar-with-dependencies.jar nju.jgd.invertedIndex.InvertedIndexDriver /user/normal4/input /user/normal4/output
# 按词频降序排列
hadoop jar Inverted_Sort_Index-jar-with-dependencies.jar nju.jgd.sortIndex.SortIndexDriver /user/normal4/wordcount /user/normal4/output

# wordcount目录下存放实验一的输出

# total_wordcount目录下存放所有文档的总词数
