package org.example;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
//        String host = parameterTool.get("host");
//        int port = parameterTool.getInt("port");
//        String host = "hadoop102";
//        int port = 7777;
//        DataStream<String> inputDataStream = env.socketTextStream(host, port);
        String inputPath = "D:\\workspace\\github\\hadoopDemo\\flink-demo\\flink-01\\src\\main\\resources\\input\\word.txt";
        DataStream<String> inputDataStream = env.readTextFile(inputPath);
        DataStream<Tuple2<String, Integer>> wordCountDataStream = inputDataStream
                .flatMap(new WordCount.MyFlatMapper())
                .keyBy(0)
                .sum(1);
        wordCountDataStream.print().setParallelism(1);
        env.execute();
    }
}
