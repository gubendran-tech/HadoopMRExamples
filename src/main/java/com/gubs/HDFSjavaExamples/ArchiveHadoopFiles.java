/**
 * 
 */
package com.gubs.HDFSjavaExamples;

/**
 * @author gubs
 *
 */
public class ArchiveHadoopFiles {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// Execute below command in hduser. Make sure mapReduce component running as HAR uses it. 

		// Syntax : hadoop archive -archiveName NAME -p <parent path> <src>* <dest>
		
		// Example : hadoop archive -archiveName gubstest.har -p /user/hduser/ sqoop_test /user/hduser/
		
		// Command to Check files in the archive
		
		// hadoop fs -lsr  har:///user/hduser/gubstest.har
		
		// To delete we need to use recursive because the underneath files under directory
		// hadoop fs -rmr  har:///user/hduser/gubstest.har
	}

}
