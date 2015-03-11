/**
 * 
 */
package com.gubs.MRNewJavaAPIExamples;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author gubs
 *
 * If you didn't mention anything in driver class all the default parameters will be initialized and job will run.
 * By default the output will be sorted and printed. Because, shuffle, sort and partition (default) will get trigger
 * Sort will happen on numbers : byte start of the file count key
 * 
 * Default of the keyValue is tab delimiter
 * 
 *  Execute : hadoop jar HadoopMRExamples.jar com.gubs.MRNewJavaAPIExamples.DefaultDriver smallfiles out19
 */
public class DefaultDriver extends Configured implements Tool {
	
	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job();
		
		job.setJobName(this.getClass().toString());
		job.setJarByClass(DefaultDriver.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// Default values are below if you don't pass
/*		job.setMapperClass(Mapper.class);
		job.setReducerClass(Reducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(1);
		job.setPartitionerClass(HashPartitioner.class);*/
		
		return (job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static void main(String[] args) throws Exception {
		if (args == null || args.length < 2) {
			System.err.print("Input is incorrect. It should be <InputDir|file> <OutputDir>");
			System.exit(0);
		}
		int exitCode = ToolRunner.run(new DefaultDriver(), args);
		System.exit(exitCode);
	}

}
