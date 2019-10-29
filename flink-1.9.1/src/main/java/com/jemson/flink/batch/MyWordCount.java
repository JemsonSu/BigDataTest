package com.jemson.flink.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

/**
 * flink java 版 wordcount
 */
public class MyWordCount {
    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);

        //String inputFile = params.get("inputFile");
        String inputFile = "./files/wc.txt";

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(params);

        DataSet<String> text = env.readTextFile(inputFile);

        //该处Tuple2 是 import org.apache.flink.api.java.tuple.Tuple2;
        FlatMapOperator<String, Tuple2<String, Integer>> operator1 = text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        });

        //使用org.apache.flink.api.java.tuple.Tuple2第一个字段分组
        UnsortedGrouping<Tuple2<String, Integer>> tuple2UnsortedGrouping = operator1.groupBy(0);
        //使用org.apache.flink.api.java.tuple.Tuple2第二个字段累加
        AggregateOperator<Tuple2<String, Integer>> sum = tuple2UnsortedGrouping.sum(1);

        sum.print();

    }
}
