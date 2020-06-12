package com.gusi.demo.mapreduce.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * MyPartitioner <br>
 *
 * @author yydeng
 * @since 2020/6/4
 */
public class MyPartitioner extends Partitioner<Text, LongWritable> {

	/**
	 * 分区
	 * 
	 * @param text K2
	 * @param longWritable V2
	 * @param i 分区个数，在job设置
	 * @return 分区
	 */
	@Override
	public int getPartition(Text text, LongWritable longWritable, int i) {
		if (text.toString().length() > 5) { // 按照单词长度分区
			return 1;
		} else {
			return 0;
		}
	}
}
