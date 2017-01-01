package com.sample.sequence;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;


public class TestAppend {
	public static void main(String args []) throws IOException, InstantiationException, IllegalAccessException {
		Configuration conf = new Configuration();
		conf.set("io.serializations",
		        "org.apache.hadoop.io.serializer.JavaSerialization");
		    conf.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
		    
		FileSystem fs = FileSystem.get(conf);
		
		Path file = new Path("/Users/prkhandelwal/Desktop/Sample/SequenceFile/sampleseq");
	    fs.delete(file, true);

	    Text key1 = new Text("Key1");
	    Text value1 = new Text("Value1");
	    Text value2 = new Text("Updated");
	    
	    SequenceFile.Metadata metadata = new SequenceFile.Metadata();
	    metadata.set(key1, value1);
	    Writer.Option metadataOption = Writer.metadata(metadata);
	    
	    Writer writer = SequenceFile.createWriter(conf,
	            SequenceFile.Writer.file(file),
	            SequenceFile.Writer.keyClass(Long.class),
	            SequenceFile.Writer.valueClass(String.class), metadataOption);
	    
	    writer.append(1L, "one");
	    writer.append(2L, "two");
	    writer.close();
	    
	    metadata.set(key1, value2);
	    
	    writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(file),
	            SequenceFile.Writer.keyClass(Long.class),
	            SequenceFile.Writer.valueClass(String.class),
	            SequenceFile.Writer.appendIfExists(true), metadataOption);
	    
	    writer.append(3L, "three");
	    writer.append(4L, "four");
	    writer.append(5L, "five");

	    writer.close();
	    
	    Reader reader = new Reader(conf, Reader.file(file));
	    
	    String value = (String) reader.getValueClass().newInstance();
	    Long key = 0L;
	    
	    while (reader.next(key) != null) {
	    	System.out.println(reader.getCurrentValue(value));
	    }
	    	    
	    reader.close();
	    
	}
}
