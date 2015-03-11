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
			// If you use below line instead of above line then you no need combiner. For 2 reasons
			// 1. Not necessary combiner. Because combiner value is not considered below. So, unwanted combiner
			// 2. Since, combiner already added / combined for the given map, since you missed taking the value the output also goes wrong.
			// You can do by having 2 files 1.txt 2.txt and have "you" in both and in 1.txt file have "you" twice. You know the difference output
			// sum += 1;
			// You can write multiple context write in reducer you want as long as same type. Each reducer output each file
			// context.write(key, sum);
		}
		context.write(key, new IntWritable(sum));
	}
}
