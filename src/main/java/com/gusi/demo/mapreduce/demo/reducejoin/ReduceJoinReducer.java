package com.gusi.demo.mapreduce.demo.reducejoin;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * ReduceJoinReducer <br>
 *
 * @author yydeng
 * @since 2020/6/11
 */
public class ReduceJoinReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// key:pid
		// values:<one,tow>

		String one = "";
		String two = "";

		for (Text v : values) {
			if (v.find(",") == 3) {
				one = v.toString();
			} else {
				two = v.toString();
			}
		}

		if (one == null || one.equals("")) {
			context.write(key, new Text("empty@" + two));
		} else if (two == null || two.equals("")) {
			context.write(key, new Text(one + "@empty"));
		} else {
			context.write(key, new Text(one + "@" + two));
		}
	}
}
