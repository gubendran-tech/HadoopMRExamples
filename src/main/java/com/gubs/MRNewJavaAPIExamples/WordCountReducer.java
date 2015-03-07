package com.gubs.MRNewJavaAPIExamples;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author gubs
 *
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	// Override reduce method
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		// Initialization. For each key reduce method calls with all the values in random order with sorted keys
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
			// You can write multiple context write in reducer you want as long as same type. Each reducer output each file
			// context.write(key, sum);
		}
		context.write(key, new IntWritable(sum));
	}
}
