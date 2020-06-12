package com.gusi.demo.mapreduce.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.gusi.demo.mapreduce.simple.WordCountMapper;
import com.gusi.demo.mapreduce.simple.WordCountReducer;

/**
 * JobMain <br>
 *
 * @author yydeng
 * @since 2020/6/4
 */
public class JobMain extends Configured implements Tool {

	@Override
	public int run(String[] strings) throws Exception {
		Job job = Job.getInstance(super.getConf(), JobMain.class.getSimpleName());
		job.setJarByClass(JobMain.class);// 打包到集群运行

		// step1:使用TextInputFormat类从hdfs读取源数据。
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path("hdfs://10.170.3.190:8020/wordcount"));

		// step2：设置Mapper的信息，k1-V1转为K2-V2；
		job.setMapperClass(WordCountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		// step3.4.5.6 分区、排序、规约、分组
		// 分区,需要保证分区数量和reducetask的数量一致
		job.setPartitionerClass(MyPartitioner.class);
		job.setNumReduceTasks(2);

		// step7:设置Reducer的信息，计算新K2-V2为K3-V3；
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		// K3：拆分的word；V2：word出现数量

		// step8:设置OutputFormat输出到hdfs。
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path("hdfs://10.170.3.190:8020/out_wordcount_2"));

		boolean b = job.waitForCompletion(true);
		return b ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		Configuration configured = new Configuration();
		Tool tool = new JobMain();

		int run = ToolRunner.run(configured, tool, args);

		System.exit(run);
	}
}
