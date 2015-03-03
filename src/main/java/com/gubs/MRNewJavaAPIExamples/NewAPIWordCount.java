/**
 * 
 */
package com.gubs.MRNewJavaAPIExamples;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * @author gubs
 *
 * This program is all about wordCount using MapReduce Framework based on Java API
 * 
 * http://www.cloudera.com/content/cloudera/en/documentation/hadoop-tutorial/CDH5/Hadoop-Tutorial/ht_wordcount1_source.html
 * http://wiki.apache.org/hadoop/WordCount
 * 
 */
public class NewAPIWordCount {

	// KeyInput takes position of the input and the line as value (Text), Map writes output key Text and value IntWritable
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		// map method needs to implement for Mapper class, Context is extended from Mapper class
		private static final IntWritable one = new IntWritable(1); 
		private Text word = new Text();
		
		public void map(LongWritable map, Text value, Context context) throws IOException, InterruptedException {
			String inputString = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(inputString);
			while(tokenizer.hasMoreTokens()) {
				String token = tokenizer.nextToken();
				word.set(token);
				// In the context only Writable wrapper interface can be set value / used to pass
				context.write(word, one);
			}
		}
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// Hadoop client will kick this job to trigger in cluster
		Job job = Job.getInstance();
		job.setJobName("wordCount");
		
		// Set the className to run on the job
		job.setJarByClass(NewAPIWordCount.class);
		
		// Output of the reducer key class and value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// Based on number of block stored in DFS no. of mapper will execute.You can set mapper count 
		// Each mapper takes a line as input and breaks it into words. It then emits a key/value pair of the word and 1. 
		// Default input is separated by a tab. (TSV -> Tab separated value)
		job.setMapperClass(Map.class);

		// As an optimization, the reducer is also used as a combiner on the map outputs. This reduces the amount of data sent across the network by combining each word into a single record. 
		job.setCombinerClass(Reduce.class);
		
		// Each reducer sums the counts for each word and emits a single key/value with the word and sum. You can pass the number of reducer to be triggered.  
		job.setReducerClass(Reduce.class);
		
		// The input format is text files and the output format is text files
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// FileInputPath and FileOutputPath
		// If you pass directory as input all the files in the directory will taken into consideration for mapper except fileName starts with Dot(.) and underscore(_)
		// Each reducer task will write the fileoutput in each file
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// Submit the job, then poll for progress until the job is complete
		job.waitForCompletion(true);
		
	}
}
