package com.gusi.demo.mapreduce.demo.flowcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * FlowCountMapper <br>
 *
 * @author Lucky
 * @since 2020/6/11
 */
public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowDTO> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] arr = value.toString().split("\\t");

		context.write(new Text(arr[1]), new FlowDTO(Integer.parseInt(arr[6]), Integer.parseInt(arr[7]), Long.parseLong(arr[8]), Long.parseLong(arr[9])));
	}
}
