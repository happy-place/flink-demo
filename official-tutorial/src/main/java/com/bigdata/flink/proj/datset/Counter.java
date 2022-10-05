package com.bigdata.flink.proj.datset;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author: huhao18@meituan.com
 * @date: 2021/5/15 4:08 下午
 * @desc:
 */
public class Counter implements Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    public void reduce(Text text, Iterator<LongWritable> iterator, OutputCollector<Text, LongWritable> outputCollector,
                       Reporter reporter) throws IOException {
        Long count = 0L;
        while (iterator.hasNext()) {
            count += iterator.next().get();
        }
        outputCollector.collect(text, new LongWritable(count));
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void configure(JobConf jobConf) {

    }
}
