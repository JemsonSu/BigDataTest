package com.jemson.flink.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

/**
 * 批处理 WC
 *
 */
public class BatchWC {
    public static void main(String[] args) throws Exception {
        ParameterTool paramets = ParameterTool.fromArgs(args); //封装参数
        String inputFile = paramets.get("inputFile"); //输入文件

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> lines = env.readTextFile(inputFile, "utf-8"); //读取文件

        FlatMapOperator<String, String> wordOperator = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> out) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });

        MapOperator<String, Tuple2<String, Long>> tuple2Operator = wordOperator.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String word) throws Exception {
                return new Tuple2<String, Long>(word, 1L);
            }
        });

        UnsortedGrouping<Tuple2<String, Long>> wordGrouping = tuple2Operator.groupBy(0); //key分组

        AggregateOperator<Tuple2<String, Long>> wordcountOperator = wordGrouping.sum(1); //key计数


        wordcountOperator.print(); //打印

        wordcountOperator.setParallelism(1); //设置并行度为1 ，下面就是一个文件，不设置就是一个目录
        wordcountOperator.writeAsText("file:///D:/test/out1", FileSystem.WriteMode.OVERWRITE);
        env.execute("BatchWC"); //有数据落地，必须要执行这句，单单打印就不要执行这句



        System.out.println("完毕！");


    }
}
