package com.sample.sequence;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;

/**
 * 
 * @author prkhandelwal
 *	Sample class file to append content to an existing sequence file
 */
public class SequenceFileAppend {
	public static void main(String args[]) throws IOException {
		Text key = new Text();
		Text value = new Text();
		
		Configuration conf = new Configuration();
		conf.set("io.serializations",
		        "org.apache.hadoop.io.serializer.WritableSerialization");

		    conf.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
		    
		FileSystem fs = FileSystem.get(conf);
		
		Path file = new Path("/Users/prkhandelwal/Desktop/Sample/SequenceFile/samplop");
	    
	    System.out.println("File exist = " + fs.exists(file));
	    
	    Text key1 = new Text("Key1");
	    Text value1 = new Text("Value1");
	    
	    SequenceFile.Metadata metadata = new SequenceFile.Metadata();
	    metadata.set(key1, value1);
	    Writer.Option metadataOption = Writer.metadata(metadata);
	    
	    Writer writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(file),
	            SequenceFile.Writer.keyClass(Text.class),
	            SequenceFile.Writer.valueClass(Text.class),
	            SequenceFile.Writer.appendIfExists(true), metadataOption);
	    
	    writer.append(new Text("Can"), new Text("Type"));
	    writer.close();
	    
	    Reader reader = new Reader(conf, Reader.file(file));
	    
	    while (reader.next(key, value)) {
	    	System.out.println(value);
	    }
	    	    
	    reader.close();
	    
	    
	}
}
