/**
 * 
 */
package com.gubs.MRNewJavaAPIExamples;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author gubs
 *
 * Map class can be created separate class. So, we can invoke with multiple driver class
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	// Map class context declared globally to avoid creation of object every time
	private static final IntWritable intValue = new IntWritable(1);
	private Text text = new Text();
	
	// Map method needs to overwrite. Input should be same as KEYIN, VALUEIN
	// LongWritable key - input of the byte lineNo record anything. Default tab delimiter
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String inputValue = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(inputValue);
		while(tokenizer.hasMoreTokens()) {
			String token = tokenizer.nextToken();
			if(Character.isLetter(token.charAt(0))) {
				text.set(token.toLowerCase());
				context.write(text, intValue);
			}
		}
	}

}
