/**
 * 
 */
package com.gubs.MRNewJavaAPIExamples;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author gubs
 * 
 *  Input for the map will be below in file
 *  blue,gubendran,lakshmanan
 *  blue,saitheja,gubendran
 *  pink,saimadhu,gubendran
 *  pink,kavitha,sekaran
 */
public class PartitionerColorMapper extends Mapper<LongWritable, Text, Text, Text>{

	private final String comma = ",";
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String inputValue = value.toString();
		String[] inputValues = inputValue.split(comma);
		
		context.write(new Text(inputValues[0]), new Text(inputValues[1] + "," + inputValues[2]));
	}
}
