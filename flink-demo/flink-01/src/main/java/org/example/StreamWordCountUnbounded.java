package org.example;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamWordCountUnbounded {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        String host = "10.0.50.100";
        int port = 7777;
        DataStream<String> inputDataStream = env.socketTextStream(host, port);
        DataStream<Tuple2<String, Integer>> wordCountDataStream = inputDataStream
                .flatMap(new WordCount.MyFlatMapper())
                .keyBy(0)
                .sum(1);
        wordCountDataStream.print().setParallelism(1);
        env.execute();
    }
}
