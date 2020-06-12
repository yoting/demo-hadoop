package com.gusi.demo.mapreduce.demo.reducejoin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * ReduceJoinMapper <br>
 *
 * @author yydeng
 * @since 2020/6/11
 */
public class ReduceJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// 读取两个文件分别map
		FileSplit inputSplit = (FileSplit) context.getInputSplit();

		String fileName = inputSplit.getPath().getName();
		String[] arr = value.toString().split(",");
		if (fileName.equals("one.txt")) {
			context.write(new Text(arr[1]), value);
		} else {
			context.write(new Text(arr[2]), value);
		}
	}
}
