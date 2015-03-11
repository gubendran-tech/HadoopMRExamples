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
			// If you use below line instead of above line then you no need combiner. For 2 reasons
			// 1. Not necessary combiner. Because combiner value is not considered below. So, unwanted combiner
			// 2. Since, combiner already added / combined for the given map, since you missed taking the value the output also goes wrong.
			// You can do by having 2 files 1.txt 2.txt and have "you" in both and in 1.txt file have "you" twice. You know the difference output
			//	sum += 1;
		}
		context.write(key, new IntWritable(sum));
	}
}
