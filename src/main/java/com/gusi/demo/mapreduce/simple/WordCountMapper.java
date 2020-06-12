package com.gusi.demo.mapreduce.simple;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * WordCountMapper <br>
 *
 * @author yydeng
 * @since 2020/6/3
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] arr = line.split(",");
		for (String word : arr) {
			context.write(new Text(word), new LongWritable(1));
		}
	}
}
