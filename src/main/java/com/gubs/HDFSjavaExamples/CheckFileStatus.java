/**
 * 
 */
package com.gubs.HDFSjavaExamples;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @author gubs
 *
 * hdfs://localhost:54310/user/hduser/colorfiles/1.txt
 * 
 * Check the file Statistics (Length, user, group and more..)
 */
public class CheckFileStatus {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length == 0) {
			System.err.println("Usage: CheckFileStatus <inputFile>");
			System.exit(-1);
		}
		String in = args[0];
		try {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(in), conf);
			FileStatus fileStat = fs.getFileStatus(new Path(in));
			System.out.println("Length of the file.." + fileStat.getLen());
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

}
