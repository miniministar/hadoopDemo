package com.exercise.hadoop.mapreduce.partitionerandwritableComparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    private FlowBean outK = new FlowBean();
    private Text outV = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 获取一行
        String line = value.toString();

        // 切割
        String[] split = line.split("\t");

        String phone = split[1];
        String up = split[split.length - 3];
        String down = split[split.length - 2];
        // 封装
        outV.set(phone);
        outK.setUpFlow(Long.parseLong(up));
        outK.setDownFlow(Long.parseLong(down));
        outK.setSumFlow();

        // 写出
        context.write(outK, outV);
    }
}
