/**
 * 
 */
package com.gubs.HDFSjavaExamples;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

/**
 * @author gubs
 *
 * hdfs://localhost:54310/user/hduser/examples/ hdfs://localhost:54310/user/hduser/colorfiles/
 * 
 *  Take path and list down all files in the path
 *  
 */
public class ListStatus {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("Usage: ListStatus <multipleInputPath> <multipleInputPath>...");
			System.exit(-1);
		}
		
		String uri = args[0];
		Configuration conf = new Configuration();
		FileSystem fst;
		try {
			fst = FileSystem.get(URI.create(uri), conf);
			
			Path[] paths = new Path[args.length];
			for (int i = 0; i < args.length; i++) {
				paths[i] = new Path(args[i]);
			}
			
			FileStatus[] fs = fst.listStatus(paths); 
			Path[] listedPaths = FileUtil.stat2Paths(fs);
			
			for (Path path : listedPaths) {
				System.out.println(path);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

}
