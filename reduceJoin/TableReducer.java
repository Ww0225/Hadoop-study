package com.ww.mapreduce.reduceJoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

/**
 * @author 28953
 * @create 2024-01-23 16:09
 */
public class TableReducer extends Reducer<Text,TableBean,TableBean,NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {

        ArrayList<TableBean> orderBeans = new ArrayList<>();
        TableBean pdBeans = new TableBean();

        // 循环遍历
        for (TableBean value : values) {

            if("order".equals(value.getFlag())){
                //订单表
                TableBean tmptableBean = new TableBean();

                try {
                    BeanUtils.copyProperties(tmptableBean,value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                orderBeans.add(tmptableBean);
            }else{
                //商品表
                try {
                    BeanUtils.copyProperties(pdBeans,value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }
        //循环表里orderBeans，赋值pdname
        for (TableBean orderBean : orderBeans) {
            orderBean.setPname(pdBeans.getPname());

            context.write(orderBean,NullWritable.get());
        }
    }
}
