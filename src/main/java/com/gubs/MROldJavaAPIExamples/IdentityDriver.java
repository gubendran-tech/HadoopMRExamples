/**
 * 
 */
package com.gubs.MROldJavaAPIExamples;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author gubs
 *
 *  Example IdentityDriver to have identityMapper and identityReducer. If you run map.reduce.tasks=0 then the map output will be written to HDFS with only identity mapper
 *  
 *  Default input KeyValue is tab separator
 *  
 *  Note : IdentityMapper is only available in old API like OutputCollector 
 *  
 *  hadoop jar HadoopMRExamples.jar com.gubs.MROldJavaAPIExamples.IdentityDriver file1.txt file2.txt out18
 * 
 */
public class IdentityDriver extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		// I like the below code from author grepalex.com
		String csvInputs = StringUtils.join(",", Arrays.copyOfRange(args, 0, args.length - 1)); 
		Path outputDir = new Path(args[args.length - 1]);
		
		JobConf jobConf = new JobConf(super.getConf());
		
		jobConf.setJobName(this.getClass().toString());
		jobConf.setJarByClass(getClass());
		
		FileInputFormat.setInputPaths(jobConf, csvInputs);
		FileOutputFormat.setOutputPath(jobConf, outputDir);
		
		jobConf.setInputFormat(KeyValueTextInputFormat.class);
		
		jobConf.setMapperClass(IdentityMapper.class);
		jobConf.setReducerClass(IdentityReducer.class);
		
		jobConf.setMapOutputKeyClass(Text.class);
		jobConf.setMapOutputValueClass(Text.class);
		
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(Text.class);
		
		return JobClient.runJob(jobConf).isSuccessful() ? 0 : 1;
		
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new IdentityDriver(), args);
		System.exit(exitCode);
	}

}
