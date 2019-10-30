package com.jemson.flink.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * scoket WC  一直累加
 */
public class MySocketWindowWC2 {
    public static void main(String[] args) throws Exception {
        //flink封装参数工具
        ParameterTool parames = ParameterTool.fromArgs(args);


        //获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String hostname = parames.get("hostname", "localhost"); //获取参数值，若没有设置就取后面默认值
        int port = parames.getInt("port");
        DataStream<String> lines = env.socketTextStream(hostname, port);

        SingleOutputStreamOperator<Tuple2<String, Long>> soso = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    out.collect(new Tuple2<>(word, 1L)); //输出键值  word-1
                }
            }
        });

        //通过Tuple第一个字段0进行分组
        KeyedStream<Tuple2<String, Long>, Tuple> ks = soso.keyBy(0);
        //通过Tuple第二个字段1进行累加
        SingleOutputStreamOperator<Tuple2<String, Long>> soso2 = ks.sum(1);


        soso2.print();

        env.execute("MySocketWindowWC2");





    }




}


