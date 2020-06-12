package com.gusi.demo.mapreduce.sorted;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * SortMapper <br>
 *
 * @author yydeng
 * @since 2020/6/5
 */
public class SortMapper extends Mapper<LongWritable, Text, PariDTO, Text> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		Counter counter = context.getCounter("MR_COUNTER", "map_counter");// 计数器，统计map执行次数
		counter.increment(1L);

		String[] arr = value.toString().split(",");
		PariDTO dto = new PariDTO();
		dto.setFirst(arr[0]);
		dto.setSecond(Integer.valueOf(arr[1]));
		context.write(dto, value);
	}
}
