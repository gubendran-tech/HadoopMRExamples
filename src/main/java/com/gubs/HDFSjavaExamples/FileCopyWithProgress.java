/**
 * 
 */
package com.gubs.HDFSjavaExamples;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

/**
 * @author gubs
 * 
 * /home/gubs/2.txt
 * hdfs://localhost:54310/user/hduser/colorfiles/2.txt
 *
 */
public class FileCopyWithProgress {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("Usage: URLCat <srcLocalFile> <destHdfsFile>");
			System.exit(-1);
		}
		
		String localFile = args[0];
		String destHdfsFile = args[1];
		FSDataOutputStream fsdo  = null;
		InputStream is = null;
		try {
			is = new BufferedInputStream(new FileInputStream(localFile));
			
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(destHdfsFile), conf);
			fsdo = fs.create(new Path(destHdfsFile), new Progressable() {
				@Override
				public void progress() {
					System.out.println(".");
					
				}
			});
			IOUtils.copyBytes(is, fsdo, 4096, false);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(is);
			IOUtils.closeStream(fsdo);
		}
	}

}
