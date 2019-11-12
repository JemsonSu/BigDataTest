package com.jemson.flink.batch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * 从Collection添加数据
 */
public class CollectionDataTest {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<String> data = new ArrayList<String>();
        data.add("jemson");
        data.add("jiao");
        data.add("jiao");
        DataSet<String> dataSet = env.fromCollection(data);

        dataSet.print();

        MapOperator<String, Tuple2<String, Integer>> kvMap = dataSet.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String key) throws Exception {
                return new Tuple2<String, Integer>(key, 1);
            }
        });

        kvMap.print();


        UnsortedGrouping<Tuple2<String, Integer>> grouping = kvMap.groupBy(0);

        AggregateOperator<Tuple2<String, Integer>> sum = grouping.sum(1);

        sum.print();

    }
}
