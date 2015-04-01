/**
 * 
 */
package com.gubs.HDFSjavaExamples;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * @author gubs
 * 
 * FSDataInputStream has method seek to set and start again to read the content or dynamically seek random read content in hadoop
 *
 * hdfs://localhost:54310/user/hduser/colorfiles/1.txt
 * 
 */
public class FileSystemDoubleCat {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length == 0) {
			System.err.println("Usage: FileSystemDoubleCat <inputFile>");
			System.exit(-1);
		}
		String uri =  args[0]; 
		Configuration conf = new Configuration();
		FSDataInputStream fsdi = null;
		try {
			FileSystem fs = FileSystem.get(URI.create(uri), conf);
			fsdi = fs.open(new Path(uri));
			IOUtils.copyBytes(fsdi, System.out, 4096, false);
			fsdi.seek(0);
			IOUtils.copyBytes(fsdi, System.out, 4096, false);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(fsdi);
		}
		
	}

}
