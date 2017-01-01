package com.sample.sequence;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;

/**
 * 
 * @author prkhandelwal
 * Sample java file to Write small files to a sequence file  
 */
public class SequenceFileWriter {
	public static void main(String args[]) throws IOException {
		Text key = new Text();
		Text value = new Text();
		
		Configuration conf = new Configuration();
		conf.set("io.serializations",
		        "org.apache.hadoop.io.serializer.WritableSerialization");

		conf.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
		    
		FileSystem fs = FileSystem.get(conf);
		
		Path inputFile = new Path("/Users/prkhandelwal/Desktop/Sample/SequenceFile/sampleTextFiles");
        Path file = new Path("/Users/prkhandelwal/Desktop/Sample/SequenceFile/samplop");
        
	    fs.delete(file, true);
	    FSDataInputStream inputStream;
	    FileStatus[] fStatus = fs.listStatus(inputFile);
	    Text key1 = new Text("Key1");
	    Text value1 = new Text("Value1");
	    
	    SequenceFile.Metadata metadata = new SequenceFile.Metadata();
	    metadata.set(key1, value1);
	    Writer.Option metadataOption = Writer.metadata(metadata);
	    
	    Writer writer = SequenceFile.createWriter(conf, 
	    		SequenceFile.Writer.file(file),
	            SequenceFile.Writer.keyClass(Text.class),
	            SequenceFile.Writer.valueClass(Text.class), metadataOption);
	    
	    for (FileStatus fst : fStatus) {
            String str = "";
            System.out.println("Processing file : " + fst.getPath().getName() + " and the size is : " + fst.getPath().getName().length());
            inputStream = fs.open(fst.getPath());
            key.set(fst.getPath().getName());
            while(inputStream.available()>0) {
                str = str+inputStream.readLine();
            }
            value.set(str);
            writer.append(key, value);

        }

	    writer.close();

	    Reader reader = new Reader(conf, Reader.file(file));
	    
	    while (reader.next(key, value)) {
	    	System.out.println(value);
	    }
	    	    
	    reader.close();
	    
	}
}
