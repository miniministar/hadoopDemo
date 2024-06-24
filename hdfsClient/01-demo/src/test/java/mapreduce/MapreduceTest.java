package mapreduce;

import com.exercise.hadoop.mapreduce.WordCountDriver;
import com.exercise.hadoop.mapreduce.WordCountMapper;
import com.exercise.hadoop.mapreduce.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

import java.io.IOException;

public class MapreduceTest {

    @Test
    public void wordCount() {
        try {
            // 1 获取配置信息以及获取 job 对象
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf);
            // 2 关 联 本 Driver 程 序 的 jar
            job.setJarByClass(WordCountDriver.class);
            // 3 关 联 Mapper 和 Reducer 的 jar
            job.setMapperClass(WordCountMapper.class);
            job.setReducerClass(WordCountReducer.class);
            // 4 设置 Mapper 输出的 kv 类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            // 5 设置最终输出 kv 类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            // 6 设置输入和输出路径
            FileInputFormat.setInputPaths(job, new Path("mapreduce/hello.txt"));
            FileOutputFormat.setOutputPath(job, new Path("mapreduce/workcount.txt"));
            // 7 提交 job
            boolean result = job.waitForCompletion(true);
            System.exit(result ? 0 : 1);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
