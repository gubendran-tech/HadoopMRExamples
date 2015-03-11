/**
 * 
 */
package com.gubs.MRNewJavaAPIExamples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author gubs
 * 
 * *  Input for the map will be below in file
 *  blue,gubendran,lakshmanan
 *  blue,saitheja,gubendran
 *  pink,saimadhu,gubendran
 *  pink,kavitha,sekaran
 *   
 *   hadoop jar HadoopMRExamples.jar com.gubs.MRNewJavaAPIExamples.ColorDriver -Dmapred.reduce.tasks=2 colorfiles  out28
 *   hadoop jar HadoopMRExamples.jar com.gubs.MRNewJavaAPIExamples.ColorDriver colorfiles  out28
 *   
 *   Output : 2 files in reducer will generate part-r-00000, part-r-00001
 *   blue in 1 file and pink in another file
 *   
 *   Ouput 2 : with default partitioner and mapred.reduce.tasks=2 we may see 1 blue and 1 pink in 1 file and another blue pink in another file.
 */
public class PartitionerColorDriver extends Configured implements Tool {

	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf);
		
		job.setJobName(this.getClass().toString());
		job.setJarByClass(this.getClass());
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setMapperClass(PartitionerColorMapper.class);
		// combiner is same as reducer to perform same operation. Its needed to reduce grouping based on key in combiner to reduce data transfer
		job.setCombinerClass(PartitionerColorReducer.class);
		job.setPartitionerClass(PartitionerColorCustom.class);
		job.setReducerClass(PartitionerColorReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return (job.waitForCompletion(true) ? 0 : 1);
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		if (args == null || args.length < 2) {
			System.err.print("Input must be 2 arguments : <inputdir|fileName> <outputdir>");
			System.exit(0);
		}
		int exitCode = ToolRunner.run(new Configuration(), new PartitionerColorDriver(), args);
		System.exit(exitCode);
	}

}
