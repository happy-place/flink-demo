package com.bigdata.flink.proj.testing.ds;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.util.Collector;

/**
 * 单词每出现2次 统计一次
 */
public class GlobalWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> localhost = see.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = localhost.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                for (String str : line.split(",")) {
                    out.collect(Tuple2.of(str, 1));
                }
            }
            //GlobalWindows 的使用需要结合trigger能使使用，因为如果你只是设置了窗口，但是没有触发，那么这个窗口没有意义
            //就如transformation算子需要一个action来触发 是一样的。
        }).keyBy(0).window(GlobalWindows.create()).trigger(CountTrigger.of(2)).sum(1);

        sum.print().setParallelism(1);

        see.execute("GlobalWindowDemo");

    }
}

