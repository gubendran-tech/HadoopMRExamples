package com.gubs.MRNewJavaAPIExamples;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 
 * @author gubs
 *
 * Custom Partitioner is use to partition based on key or value and provide reducer int value mentioned which reducer should pick for which input (key | value)
 * 
 */
public class PartitionerColorCustom extends Partitioner<Text, Text>{

	@Override
	public int getPartition(Text key, Text value, int numReduceTasks) {
		// If 0 | 1 numReduceTasks then return 0 to do partition everything in same
		if (numReduceTasks == 0 || numReduceTasks == 1) {
			return 0;
		}
		// partition value return should be starts with 0
		if (key.toString().equalsIgnoreCase("pink")) {
			return 0;
		}
		if (key.toString().equalsIgnoreCase("blue")) {
			return 1;
		}
		return 0;
	}

}
