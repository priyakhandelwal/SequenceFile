package com.sample.sequence.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Value implements WritableComparable<Value> {
	String metadata;
	String content;
	
	public Value() {
		metadata = "sample";
		content = "content";
	}
	
	public String getMetadata() {
		return metadata;
	}
	public void setMetadata(String metadata) {
		this.metadata = metadata;
	}
	public String getContent() {
		return content;
	}
	public void setContent(String content) {
		this.content = content;
	}
	public void write(DataOutput out) throws IOException {
		out.writeUTF(metadata);
		out.writeUTF(content);
		
	}
	public void readFields(DataInput in) throws IOException {
		metadata = in.readUTF();
		content = in.readUTF();
		
	}
	public int compareTo(Value o) {
		return this.metadata.compareTo(o.metadata);
	}
	
	@Override
	public String toString() {
		return "metadata = " + this.metadata + "\ncontent = " + this.content; 
	}
}
