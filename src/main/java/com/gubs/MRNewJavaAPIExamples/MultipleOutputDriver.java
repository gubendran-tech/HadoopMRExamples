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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author gubs
 *
 * This class creates multiple output files calling multipleOutputReducer for the input
 * 
 * Hadoop in practice author well said : Reference : http://grepalex.com/2013/05/20/multipleoutputs-part1/
 * 
 * MultipleOutput format is advantage compare to partitioner where you no need to decide upfront the partitions code you are going to make and the reducer tasks
 * 
 */
public class MultipleOutputDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf);
		
		job.setJobName(this.getClass().toString());
		job.setJarByClass(this.getClass());
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setInputFormatClass(TextInputFormat.class);
		MultipleOutputs.addNamedOutput(job, "test", TextOutputFormat.class, Text.class, IntWritable.class);
		
		job.setMapperClass(WordCountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(MultipleOutputReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		return (job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static void main(String[] args) throws Exception {
		if (args == null || args.length < 2) {
			System.err.print("Input should be 2 arguments : <InputDir|InputFiles> <outputdir>");
			System.exit(0);
		}
		
		int exitCode = ToolRunner.run(new Configuration(), new MultipleOutputDriver(), args);
		System.exit(exitCode);
	}

}
