/**
 * 
 */
package com.gubs.HDFSjavaExamples;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;


/**
 * @author gubs
 *
 */
public class OutputStreamJavaExample {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String outFile = args[0];
		try {
			OutputStream out = new FileOutputStream(new File(outFile));
			out.write("Content".getBytes());
			out.close();
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
