package com.gusi.demo.mapreduce.demo.flowcount.sorted;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * FlowPartation <br>
 *
 * @author Lucky
 * @since 2020/6/11
 */
public class FlowPartation extends Partitioner<FlowDTO2, Text> {
	@Override
	public int getPartition(FlowDTO2 flowDTO, Text text, int i) {
		if (text.toString().startsWith("13")) {
			return 0;
		} else if (text.toString().startsWith("18")) {
			return 1;
		} else {
			return 2;
		}
	}
}
