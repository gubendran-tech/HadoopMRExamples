/**
 * 
 */
package com.gubs.MROldJavaAPIExamples;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author gubs
 *
 * MultipleOutputFormat Example as per author alex 
 * 
 */
public class MOFExample extends Configured implements Tool {
	static class KeyBasedMultipleOuputFormat extends MultipleTextOutputFormat<Text, Text> {
		// name is to keep the same output filename as it is with the output 
		@Override
        protected String generateFileNameForKeyValue(Text key, Text value, String name) {
            return key.toString() + "/" + name;
        }
		
		// Get the input fileName and append with the output name part-...
		@Override
		protected String getInputFileBasedOutputFileName(JobConf jobConf, String name) {
			String inFileName = new Path(jobConf.get("map.input.file")).getName();
			return name + "-" + inFileName;
 		}
		
		// Generate the actual key from the given key and value.
		@Override
		protected Text generateActualKey(Text key, Text value) {
			return null;
		}
		
		// Generate the actual value from the given key and value.
		@Override
		protected Text generateActualValue(Text key, Text value) {
			return value;
		}
		
		// Generate the leaf name for the output file name.
		@Override
		protected String generateLeafFileName(String name) {
			return name;
		}
		
		//  Create a composite record writer that can write key/value data to different output files
		@Override
		public RecordWriter<Text, Text> getRecordWriter(FileSystem fs, JobConf job, String name, Progressable progressable) throws IOException {
			if (name.startsWith("apple")) {
				return new TextOutputFormat<Text, Text>().getRecordWriter(fs, job, name, progressable);
			} else if (name.startsWith("banana")) {
				return new TextOutputFormat<Text, Text>().getRecordWriter(fs, job, name, progressable);
			}
			return super.getRecordWriter(fs, job, name, progressable) ;
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		JobConf jobConf = new JobConf(super.getConf());
		
		jobConf.setJarByClass(this.getClass());
		jobConf.setJobName(this.getClass().toString());
		jobConf.setMapperClass(IdentityMapper.class);
		
		jobConf.setInputFormat(KeyValueTextInputFormat.class);
		jobConf.setOutputFormat(KeyBasedMultipleOuputFormat.class);
		
		/*
		jobConf.setMapOutputKeyClass(Text.class);
		jobConf.setMapOutputValueClass(Text.class);*/
		
		FileInputFormat.setInputPaths(jobConf, StringUtils.join(Arrays.copyOfRange(args, 0, args.length - 1), ","));
		FileOutputFormat.setOutputPath(jobConf, new Path(args[args.length - 1]));
		
		return JobClient.runJob(jobConf).isSuccessful() ? 0 : 1;
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		if (args == null || args.length < 2) {
			System.err.println("Input is mandatory either <InputDir|InputFiles> <outputDir>");
			System.exit(0);
		}
		int exitCode = ToolRunner.run(new Configuration(), new MOFExample(), args);
		System.exit(exitCode);
	}

}
