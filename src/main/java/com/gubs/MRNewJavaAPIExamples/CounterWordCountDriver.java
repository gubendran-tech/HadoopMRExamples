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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * @author gubs
 * * Use userDefined counter in the map to identify number of bad records failed to process using counter.
 * So in the execution of the program you can see the percentage of records failed with input record
 * New MRV1 counter implemented with Context earlier it implemented with Reporter. But, the logic not changed
 * 
 * Increment the counter by 1. You can implement counter in mapper are reducer any where..
 */
public class CounterWordCountDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf);
		
		job.setJobName(this.getClass().toString());
		job.setJarByClass(this.getClass());
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(CounterWordCountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setCombinerClass(WordCountReducer.class);
		
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		return (job.waitForCompletion(true) ? 0 : 1);
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		if (args == null || args.length < 2) {
			System.err.print("Input should be 2 arguments : <inputDir|inputFiles> <outputDir>");
			System.exit(0);
		}
		int exitCode = ToolRunner.run(new Configuration(), new CounterWordCountDriver(), args);
		System.exit(exitCode);
	}

}
