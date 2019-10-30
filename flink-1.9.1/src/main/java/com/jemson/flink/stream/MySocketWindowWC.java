package com.jemson.flink.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 窗口函数统计scoket WC
 * 采用自定义类
 */
public class MySocketWindowWC {
    public static void main(String[] args) throws Exception {
        //flink封装参数工具
        ParameterTool parames = ParameterTool.fromArgs(args);


        //获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String hostname = parames.get("hostname", "localhost"); //获取参数值，若没有设置就取后面默认值
        int port = parames.getInt("port");
        DataStream<String> lines = env.socketTextStream(hostname, port);

        DataStream<MyWordCount> wordDS = lines.flatMap(new FlatMapFunction<String, MyWordCount>() {
            @Override
            public void flatMap(String line, Collector<MyWordCount> out) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    out.collect(new MyWordCount(word, 1));
                }
            }
        });

        KeyedStream<MyWordCount, Tuple> ks = wordDS.keyBy("word");

        //窗口大小3s  每2s执行一次统计
        WindowedStream<MyWordCount, Tuple, TimeWindow> ws = ks.timeWindow(Time.seconds(3), Time.seconds(2));

        SingleOutputStreamOperator<MyWordCount> count = ws.sum("count");


        count.print();

        env.execute("MyStream of Windows"); //执行任务



    }


    public static class MyWordCount {
        public String word;
        public long count;

        public MyWordCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        public MyWordCount() {
        }



        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return "MyWordCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }

}


