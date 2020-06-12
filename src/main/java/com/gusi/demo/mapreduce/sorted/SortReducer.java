package com.gusi.demo.mapreduce.sorted;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * SortReducer <br>
 *
 * @author yydeng
 * @since 2020/6/5
 */
public class SortReducer extends Reducer<PariDTO, Text, PariDTO, NullWritable> {
	@Override
	protected void reduce(PariDTO key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		// 计数器，通过枚举实现
		Counter counterKey = context.getCounter(MR_COUNTER_ENUM.MR_COUNT_KEY);
		counterKey.increment(1L);

		Counter counterValue = context.getCounter(MR_COUNTER_ENUM.MR_COUNT_VALUE);

		// 可能有重复，需要多次输出
		Iterator<Text> iterator = values.iterator();
		while (iterator.hasNext()) {
			iterator.next();
			context.write(key, NullWritable.get());

			counterValue.increment(1L);
		}
	}
}

enum MR_COUNTER_ENUM {
	MR_COUNT_KEY, MR_COUNT_VALUE
}
