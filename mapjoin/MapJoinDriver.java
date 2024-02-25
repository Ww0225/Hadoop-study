package com.ww.mapreduce.mapjoin;

import com.ww.mapreduce.reduceJoin.TableBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


/**
 * @author 28953
 * @create 2024-01-28 16:12
 */
public class MapJoinDriver {
    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(MapJoinDriver.class);

        job.setMapperClass(MapJoinMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);

        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.addCacheArchive(new URI("file:///D:/hadoop_text/input/reducejoin/pd.txt"));

        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job,new Path("D:\\hadoop_text\\input\\reducejoin\\order.txt"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\hadoop_text\\output\\output10"));

        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
