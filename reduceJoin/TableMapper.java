package com.ww.mapreduce.reduceJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author 28953
 * @create 2024-01-23 15:34
 */
public class TableMapper extends Mapper<LongWritable,Text,Text,TableBean> {

    private String filename;
    private Text outK = new Text();
    private TableBean outV = new TableBean();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 初始化  order  pd
        FileSplit split = (FileSplit) context.getInputSplit();

        filename = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.获取一行
        String line = value.toString();

        //2.判断是哪个文件的
        if(filename.contains("order")){
            //处理的是订单表
            String[] split = line.split("\t");

            // 封装 K V
            outK.set(split[1]);
            outV.setId(split[0]);
            outV.setPid(split[1]);
            outV.setAmount(Integer.parseInt(split[2]));
            outV.setPname("");
            outV.setFlag("order");

        }else{
            //处理的是商品表
            String[] split = line.split("\t");

            outK.set(split[0]);
            outV.setId("");
            outV.setPid(split[0]);
            outV.setAmount(0);
            outV.setPname(split[1]);
            outV.setFlag("pd");
        }

        //写出
        context.write(outK,outV);
    }
}