package com.gusi.demo.mapreduce.demo.flowcount.sorted;

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
public class FlowCountMapper2 extends Mapper<LongWritable, Text, FlowDTO2, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] line = value.toString().split("\t");
		String[] arr = line[1].split(",");

		FlowDTO2 flow = new FlowDTO2();
		flow.setUpPkgs(Integer.parseInt(arr[0]));
		flow.setDownPkgs(Integer.parseInt(arr[1]));
		flow.setUpFlows(Long.parseLong(arr[2]));
		flow.setDownFlows(Long.parseLong(arr[3]));
		context.write(flow, new Text(line[0]));
	}
}
