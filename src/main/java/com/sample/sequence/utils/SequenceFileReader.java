package com.sample.sequence.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

/**
 * 
 * @author prkhandelwal
 *	Sample java file to read content of a sequence file.
 */
public class SequenceFileReader{
	public Value getContent(String filename) {
		Configuration conf = new Configuration();
		LocationMapper locationMapper = new LocationMapper();
		long location;
		
		Value result = null;
		conf.set("io.serializations",
		        "org.apache.hadoop.io.serializer.WritableSerialization");

		conf.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
		Path path = new Path("/Users/prkhandelwal/Desktop/Sample/SequenceFile/storage");
	
		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));
			Text key = new Text();
			Value value = new Value();
			location = locationMapper.getKeyLocation(filename);
			reader.seek(location);
			
			while (reader.next(key, value)) {
				if ((key.toString()).equals(filename)) {		
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