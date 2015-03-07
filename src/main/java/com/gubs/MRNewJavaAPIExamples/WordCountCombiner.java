/**
 * 
 */
package com.gubs.MRNewJavaAPIExamples;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author gubs
 *
 * WordCount combiner task is to reduce the burden of reducer to avoid transfering lot of data in network bandwidth
 * This class extends same reducer class with reduce() method. For each map task output this combiner runs and provide output.
 * output comes in random order each time it runs. But, the output is same. 
 * Commutative and associative this is applicable.
 * commutative -> a + b = b + a or associative -> (a+b)+c= a+(b+c) 
 */
public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		context.write(key, new IntWritable(sum));
	}
}
