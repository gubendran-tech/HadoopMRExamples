/**
 * 
 */
package com.gubs.MRNewJavaAPIExamples;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author gubs
 *
 * We need combiner. Because combiner can combine same key (same color) and combine values and give to reducer from the map
 * Having said we can use reducer class itself as combiner class because logic we need in combiner also same
 * 
 */
public class PartitionerColorReducer extends Reducer<Text, Text, Text, Text>{
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for (Text value : values) {
			context.write(key, value);
		}
	}

}
