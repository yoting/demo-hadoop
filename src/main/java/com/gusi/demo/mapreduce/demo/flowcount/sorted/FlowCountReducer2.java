package com.gusi.demo.mapreduce.demo.flowcount.sorted;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * FlowCountReducer <br>
 *
 * @author Lucky
 * @since 2020/6/11
 */
public class FlowCountReducer2 extends Reducer<FlowDTO2, Text, Text, FlowDTO2> {
	@Override
	protected void reduce(FlowDTO2 key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		context.write(values.iterator().next(), key);
	}
}
