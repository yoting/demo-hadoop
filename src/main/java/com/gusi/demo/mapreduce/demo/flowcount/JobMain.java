package com.gusi.demo.mapreduce.demo.flowcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JobMain <br>
 *     流量统计
 *
 * @author yydeng
 * @since 2020/6/4
 */
public class JobMain extends Configured implements Tool {

	private static final Logger LOGGER = LoggerFactory.getLogger(JobMain.class);

	@Override
	public int run(String[] strings) throws Exception {
		LOGGER.info("Start job run...");
		Job job = Job.getInstance(super.getConf(), JobMain.class.getSimpleName());
		job.setJarByClass(JobMain.class);// 打包到集群运行

		// step1:使用TextInputFormat类从hdfs读取源数据。
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path("d:\\hadoop-hdfs\\input\\flowcount\\data_flow.dat"));

		// step2：设置Mapper的信息，k1-V1转为K2-V2；
		job.setMapperClass(FlowCountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowDTO.class);

		// step3.4.5.6 分区、排序、规约、分组

		// step7:设置Reducer的信息，计算新K2-V2为K3-V3；
		job.setReducerClass(FlowCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowDTO.class);

		// step8:设置OutputFormat输出到hdfs。
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path("d:\\hadoop-hdfs\\output\\flowcount"));

		boolean b = job.waitForCompletion(true);
		return b ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		Configuration configured = new Configuration();
		// configured.set("mapreduce.framework.name", "local");
		configured.set("fs.defaultFS","file:///");
		Tool tool = new JobMain();

		int run = ToolRunner.run(configured, tool, args);

		System.exit(run);
	}
}
