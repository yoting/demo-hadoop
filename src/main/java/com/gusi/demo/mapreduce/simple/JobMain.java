package com.gusi.demo.mapreduce.simple;

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

/**
 * JobMain <br>
 *
 * @author yydeng
 * @since 2020/6/3
 */
public class JobMain extends Configured implements Tool {

	@Override
	public int run(String[] strings) throws Exception {
		Job job = Job.getInstance(super.getConf(), JobMain.class.getSimpleName());
		job.setJarByClass(JobMain.class);// 打包到集群运行

		// step1:使用TextInputFormat类从hdfs读取源数据。
		/**
		 * 读取的结果是K1-V1；K1：hdfs文件内容偏移量；V1：hdfs的一行数据
		 * k1:v1
		 * 0 :hello,hadoop
		 * 12:hello,java,hadoop
		 * 30:hello,hdfs
		 */
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path("hdfs://10.170.3.190:8020/wordcount"));
		//注意：此处会读取该文件夹下所有文件

		// step2：设置Mapper的信息，k1-V1转为K2-V2；
		/**
		 * K2：拆分的word；V2：常量1
		 * k2:v2
		 * hello:1
		 * hadoop:1
		 * hello:1
		 * java:1
		 * hadoop:1
		 * hello:1
		 * hdfs:1
		 */
		job.setMapperClass(WordCountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		// Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>已经有了泛型，为什么此处还需要再设置？
		// 泛型擦除，另外Mapper可以不写泛型。个人觉得此处可以优化，在设置MapperClass的时候，自动把泛型给job设置好。

		// step3.4.5.6 分区、排序、规约、分组
		/**
		 * Shuffle阶段会产生新的K2-V2
		 * 新K2：新V2
		 * hello:<1,1,1>
		 * hadoop:<1,1>
		 * java:<1>
		 * hdfs:<1>
		 */

		// step7:设置Reducer的信息，计算新K2-V2为K3-V3；
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		// K3：拆分的word；V2：word出现数量

		// step8:设置OutputFormat输出到hdfs。
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path("hdfs://10.170.3.190:8020/out_worldcount"));

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
