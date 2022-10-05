package com.bigdata.flink.proj.datset;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * @author: huhao18@meituan.com
 * @date: 2021/5/15 4:09 下午
 * @desc:
 */
public class Tokenizer implements Mapper<LongWritable, Text, Text, LongWritable> {
    @Override
    public void map(LongWritable longWritable, Text text, OutputCollector<Text, LongWritable> outputCollector,
                    Reporter reporter) throws IOException {
        String[] split = text.toString().split(",");
        for (String str : split) {
            outputCollector.collect(new Text(str), new LongWritable(1L));
        }
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void configure(JobConf jobConf) {

    }
}
