package org.apache.flink.playgrounds.ops.clickcount.sink;

import org.apache.flink.playgrounds.ops.clickcount.records.ClickEventStatistics;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class KafkaMockSink implements SinkFunction<ClickEventStatistics> {

    @Override
    public void invoke(ClickEventStatistics value, Context context) throws Exception {
        System.out.println(value.toString());
    }
}
