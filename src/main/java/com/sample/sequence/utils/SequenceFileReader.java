package com.sample.sequence.utils;

import java.io.IOException;

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
	public String getContent(String filename) {
		Configuration conf = new Configuration();
		String content = null;
		conf.set("io.serializations",
		        "org.apache.hadoop.io.serializer.WritableSerialization");

		conf.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
		FileSystem fs;
		Path path = new Path("/Users/prkhandelwal/Desktop/Sample/SequenceFile/samplop");
		
		try {
			fs = FileSystem.get(conf);
			System.out.println("Sequence File exist = " + fs.exists(path));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));
			Text key = new Text();
			Text value = new Text();
			while (reader.next(key, value)) {
				if ((key.toString()).equals(filename)) {
					System.out.println("Found value = " + value + "for key =" + key.toString());
					content = value.toString();
					break;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return content;
	}
}