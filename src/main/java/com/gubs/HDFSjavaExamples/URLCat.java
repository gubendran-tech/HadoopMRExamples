/**
 * 
 */
package com.gubs.HDFSjavaExamples;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

/**
 * @author gubs
 *
 * Read file from HDFS
 * 
 * hdfs://localhost:54310/user/hduser/colorfiles/1.txt
 * 
 */
public class URLCat {

	static {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length == 0) {
			System.err.println("Usage: URLCat <inputFile>");
			System.exit(-1);
		}
		InputStream ins = null;
		try {
			 ins = new URL(
					args[0])
					.openStream();
			/*BufferedReader br = new BufferedReader(new InputStreamReader(ins));
			String line = br.readLine();
			while (line != null) {
				System.out.println(line);
				line = br.readLine();
			}*/
			 IOUtils.copyBytes(ins, System.out, 4096, false);
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(ins);
		}

	}

}
