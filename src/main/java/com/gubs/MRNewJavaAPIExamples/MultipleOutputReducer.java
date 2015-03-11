/**
 * 
 */
package com.gubs.MRNewJavaAPIExamples;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * @author gubs
 *
 * MultipleOutputReducer format class used in reducer to produce multiple outputs in the wordCount based on the 1st character word. 
 * You can name the reducer fileName through the reducer output with multiple files
 */
public class MultipleOutputReducer extends Reducer<Text, Iterable<IntWritable>, Text, IntWritable>{

	private MultipleOutputs<Text, IntWritable> multipleOutputs;
	
	// override for multipleOutput initializer
	/**
	 * Setup method calls only once for each reducer and each mapper
	 */
	@Override
	public void setup(Context context) {
		multipleOutputs = new MultipleOutputs<Text, IntWritable>(context);
	}
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		// Instead of regular context write the context on multipleOutputContext by override
		//context.write(key, new IntWritable(sum));
		multipleOutputs.write("test", key, new IntWritable(sum), key.toString().substring(0, 1));
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		multipleOutputs.close();
	}
}
