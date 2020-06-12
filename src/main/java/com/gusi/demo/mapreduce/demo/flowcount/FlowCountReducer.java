package com.gusi.demo.mapreduce.demo.flowcount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * FlowCountReducer <br>
 *
 * @author Lucky
 * @since 2020/6/11
 */
public class FlowCountReducer extends Reducer<Text, FlowDTO, Text, FlowDTO> {
	@Override
	protected void reduce(Text key, Iterable<FlowDTO> values, Context context) throws IOException, InterruptedException {
		FlowDTO result = new FlowDTO();
		for (FlowDTO flow : values) {
			result.setUpPkgs(result.getUpPkgs() + flow.getUpPkgs());
			result.setDownPkgs(result.getDownPkgs() + flow.getDownPkgs());
			result.setUpFlows(result.getUpFlows() + flow.getUpFlows());
			result.setDownFlows(result.getDownFlows() + flow.getDownFlows());
		}

		context.write(key, result);
	}
}
