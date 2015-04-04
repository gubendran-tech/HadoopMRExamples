/**
 * 
 */
package com.gubs.HDFSjavaExamples;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;

/**
 * @author gubs
 *
 * hdfs://localhost:54310/user/hduser/colorfiles/dummy.txt
 *
 */
public class FSDataOutputStreamExample {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Usage: FSDataOutputStreamExample <outFile>");
			System.exit(-1);
		}
		String uri = args[0];
		Configuration conf = new Configuration();
		try {
			FileSystem fs = FileSystem.get(URI.create(uri), conf);
			Path path = new Path(uri);
			FSDataOutputStream out = fs.create(path, new Progressable() {
				
				@Override
				public void progress() {
					System.out.println(".");
				}
			});
			out.write("Content".getBytes("UTF-8"));
			out.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

}
