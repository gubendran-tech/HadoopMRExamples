/**
 * 
 */
package com.gubs.HDFSjavaExamples;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * @author gubs
 * 
 * Incase you cannot use set FSStreamHandler you can use good way of FileSystem class to read file content as Java
 * Except use new Path() instead java.io.File
 * 
 * hdfs://localhost:54310/user/hduser/colorfiles/1.txt
 * 
 */
public class FileSystemCat {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length == 0) {
			System.err.println("Usage: FileSystemCat <inputFile>");
			System.exit(-1);
		}
		Configuration conf = new Configuration();
		InputStream in = null;
		try {
			String uri = args[0];
			FileSystem fs = FileSystem.get(URI.create(uri), conf);
			
			in = fs.open(new Path(uri));
			IOUtils.copyBytes(in, System.out, 4096, false);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(in);
		}
		

	}

}
