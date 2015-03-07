/**
 * 
 */
package com.gubs.MRNewJavaAPIExamples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author gubs
 *
 * Write Driver class based on ToolRunner. 
 * Because ToolRunner take the configuration passed from commandLine and pass to the job no need to pass configuration in code
 *  
 *   To Run in shell : 
 *   hadoop jar HadoopMRExamples.jar com.gubs.MRNewJavaAPIExamples.WordCountToolRunner smallfiles out9
 *   hadoop jar HadoopMRExamples.jar com.gubs.MRNewJavaAPIExamples.WordCountToolRunner -Dmapred.reduce.tasks=0 smallfiles out9 
 *   hadoop jar HadoopMRExamples.jar com.gubs.MRNewJavaAPIExamples.WordCountToolRunner -Dmapred.reduce.tasks=2 smallfiles out10
 */
public class WordCountToolRunner extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		// configuration and jobName passed to constructor
		Job job = new Job(conf, this.getClass().toString());
		
		job.setJarByClass(WordCountToolRunner.class);
		
		// Optional
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// output part-m-0000 m => mapper r => reducer. 0000 move to 0001 0002 based on number of files
		job.setMapperClass(WordCountMapper.class);
		// If reducer task mentioned to 0 then combiner class also won't trigger.
		// Because combiner class purpose is to reduce the network bandwidth of reducer data input
		job.setCombinerClass(WordCountCombiner.class);
		// Based on mapred.reduce.tasks the number reducer splits and paritioned properly but output is same. 
		// If 2 reducer 2 files will be stored. But, input to reducer goes 1 key with all the values. Output never changes
		job.setReducerClass(WordCountReducer.class);
		
		return (job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new WordCountToolRunner(), args);
		System.exit(exitCode);
	}

}
