package com.bigdata.flink.proj.datset;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapred.HadoopInputFormat;
import org.apache.flink.api.java.hadoop.mapred.HadoopOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.hadoopcompatibility.mapred.HadoopMapFunction;
import org.apache.flink.hadoopcompatibility.mapred.HadoopReduceCombineFunction;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

/**
 * @author: huhao18@meituan.com
 * @date: 2021/5/11 1:17 下午
 * @desc: flink data set api 运行 wordcount
 */


public class HadoopSupportApp {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String base = env.getClass().getClassLoader().getResource("data/wc3.txt").getPath().split("target")[0];

        String inputPath = base+"/src/main/resources/data/wc3.txt";
        String outputPath = base+"/src/main/resources/output";

        JobConf jobConf = new JobConf();

        HadoopInputFormat<LongWritable, Text> hadoopIF = new HadoopInputFormat(
                new TextInputFormat(), LongWritable.class, Text.class, jobConf
        );
        TextInputFormat.addInputPath(jobConf, new Path(inputPath));

        DataSet<Tuple2<LongWritable, Text>> text = env.createInput(hadoopIF);

        DataSet<Tuple2<Text, LongWritable>> result = text
                .flatMap(new HadoopMapFunction(new Tokenizer()))
                .groupBy(0)
                .reduceGroup(new HadoopReduceCombineFunction(
                        new Counter(), new Counter()
                ));

        HadoopOutputFormat<Text, LongWritable> hadoopOF = new HadoopOutputFormat<Text, LongWritable>(
                new TextOutputFormat(), jobConf
        );
        hadoopOF.getJobConf().set("mapreduce.output.textoutputformat.separator", " ");
        TextOutputFormat.setOutputPath(jobConf, new Path(outputPath));

        result.output(hadoopOF);

        env.execute("Hadoop WordCount");
    }

}


