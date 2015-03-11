package com.gubs.MRNewJavaAPIExamples;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author gubs
 *
 * Use userDefined counter in the map to identify number of bad records failed to process using counter.
 * So in the execution of the program you can see the percentage of records failed with input record
 * New MRV1 counter implemented with Context earlier it implemented with Reporter. But, the logic not changed
 * 
 * Increment the counter by 1. You can implement counter in mapper are reducer any where..
 */
public class CounterWordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	private IntWritable writableValue = new IntWritable(1);
	private Text text = new Text();
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String inputValue = value.toString();
		StringTokenizer tokens = new StringTokenizer(inputValue);
		while(tokens.hasMoreTokens()) {
			String token = tokens.nextToken();
			if (Character.isLetter(token.charAt(0))) {
				text.set(token);
				context.write(text, writableValue);
			} else {
				context.getCounter("BadWordCounter", "badCounter").increment(1);
			}
		}
	}
}
