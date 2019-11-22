package com.jemson.flink.table;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * flink table batch版 WC
 */
public class WCTable {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //通ExecutionEnvironment创建BatchTableEnvironment
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);




        DataSet<WC> dataSet = env.fromElements(new WC[]{new WC("Hello", 1L), new WC("Ciao", 1L), new WC("Hello", 1L)});

        Table table = tableEnv.fromDataSet(dataSet);

        Table table2 = table.groupBy("word")
                .select("word, frequency.sum as frequency");

        table2.printSchema();

        DataSet<WC> dataSet2 = tableEnv.toDataSet(table2, WC.class);

        dataSet2.print();


    }



    public static class WC
    {
        public String word;
        public long frequency;

        public WC()
        {
        }

        public WC(String word, long frequency)
        {
            this.word = word;
            this.frequency = frequency;
        }

        public String toString()
        {
            return "WC " + this.word + " " + this.frequency;
        }
    }







}

