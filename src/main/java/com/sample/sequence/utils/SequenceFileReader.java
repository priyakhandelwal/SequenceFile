package com.sample.sequence.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * 
 * @author prkhandelwal
 *	Sample java file to read content of a sequence file.
 */
public class SequenceFileReader{
	public Value getContent(String filename) {
		Configuration conf = new Configuration();
//		String content = null;
		Value result = null;
		conf.set("io.serializations",
		        "org.apache.hadoop.io.serializer.WritableSerialization");

		conf.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
		FileSystem fs;
		Path path = new Path("/Users/prkhandelwal/Desktop/Sample/SequenceFile/seqwithmetainvalue");
		
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
			Value value = new Value();

			while (reader.next(key, value)) {
				System.out.println("key filename " + key);
				System.out.println("value metadata= " + value.getMetadata());
				System.out.println("value content = " + value.getContent());

				if ((key.toString()).equals(filename)) {
					System.out.println("Found value = " + value + "for key =" + key.toString());
					result = value;
					break;
				}
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}
}