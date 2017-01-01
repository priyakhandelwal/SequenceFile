package com.sample.sequence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

/**
 * 
 * @author prkhandelwal
 *	Sample java file to read content of a sequence file.
 */
public class SequenceFileReader{
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("io.serializations",
		        "org.apache.hadoop.io.serializer.WritableSerialization");

		conf.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("/Users/prkhandelwal/Desktop/Sample/SequenceFile/samplop");
		
		System.out.println("File exist = " + fs.exists(path));
		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));
			Text key = new Text();
			Text value = new Text();
			while (reader.next(key, value)) { System.out.println(key);
			System.out.println(value);
			}
		} finally {
			reader.close();
		}
	}
}