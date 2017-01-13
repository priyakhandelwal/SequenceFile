package com.sample.sequence.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;

/**
 * 
 * @author prkhandelwal
 *	Sample class file to append content to an existing sequence file
 */
public class SequenceFileAppend {
	
	public void createfile(Create create) throws IOException {
		long byteoffset;
		String filePath = "/Users/prkhandelwal/Desktop/Sample/SequenceFile/storage";
		
		Text key = new Text();
		Value value = new Value();
		Configuration conf = new Configuration();
		FileUtil fileutil = new FileUtil();
		LocationMapper locationMapper = new LocationMapper();
		
		conf.set("io.serializations",
		        "org.apache.hadoop.io.serializer.WritableSerialization");

		conf.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
		
		Path file = new Path(filePath);
	    
	    SequenceFile.Metadata metadata = new SequenceFile.Metadata();
	    metadata.set(new Text("Key1"), new Text("Value1"));
	    Writer.Option metadataOption = Writer.metadata(metadata);
	    
	    Writer writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(file),
	            SequenceFile.Writer.keyClass(Text.class),
	            SequenceFile.Writer.valueClass(Value.class),
	            SequenceFile.Writer.appendIfExists(true), metadataOption);
	    
	    byteoffset = fileutil.getByteOffset(filePath);
	    locationMapper.saveLocation(create.getPathName(), byteoffset);
	    
	    key.set(create.getPathName());
	    value.setMetadata(create.getMetadata());
	    value.setContent(create.getContent());
	    writer.append(key, value);
	    writer.close();
	}
}
