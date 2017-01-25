package com.paypal.storageImpl;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.util.Map;
import java.util.Properties;
import com.paypal.storage.Storage;

public class StorageImpl implements Storage{
	
	private String path = "/Users/prkhandelwal/Desktop/Sample/Dat-output/storage.dat";
	private String mapperFile = "/Users/prkhandelwal/Desktop/Sample/Dat-storage/storage-mapper.properties";
	
	public boolean create(String filename, byte[] content, Map<String, String> metadata) throws IOException {
		boolean isCreated = false;
		byte[] meta = getByteArrayMetadata(metadata);
		isCreated = writeToFile(filename, path, meta, content);
		return isCreated;
	}
	
	public byte[] read(String filename) throws FileNotFoundException {
		long contentLocation;
		int contentSize;
		byte [] content = null;
		RandomAccessFile file ;
		try {
			contentLocation = getContentLocation(filename);
			contentSize = getContentLength(filename);
			content = new byte[contentSize];
			file = new RandomAccessFile(path, "r");
			file.seek(contentLocation);
			file.read(content);
			file.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		return content;
		
	}
	
	public byte[] getMetadata(String filename) {
		long metaLocation;
		int metaSize;
		byte [] metadata = null;
		RandomAccessFile file ;
		try {
			metaLocation = getMetaLocation(filename);
			metaSize = getMetaLength(filename);
			metadata = new byte[metaSize];
			file = new RandomAccessFile(path, "r");
			file.seek(metaLocation);
			file.read(metadata);
			file.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return metadata;
		
	}
	
	private boolean writeToFile(String filename, String path, byte[] metadata, byte[] content) throws IOException {
		long contentLocation;
		long metalocation;
		
		RandomAccessFile file = new RandomAccessFile(path, "rw");
		file.seek(file.length());
		metalocation = file.length();
		file.write(metadata);
		contentLocation = file.getFilePointer();
		file.write(content);
		saveLengthAndLocation(filename, metalocation, metadata.length, contentLocation, content.length);
		file.close();
		return true;
	}

	private byte[] getByteArrayMetadata(Map<String, String> map) throws IOException {
		ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
		ObjectOutputStream out = new ObjectOutputStream(byteOut);
		out.writeObject(map);
		out.close();
		return byteOut.toByteArray();
	}
	
	private void saveLengthAndLocation(String key, long Metalocation, int Metalength, long contentLocation, int contentLength) throws IOException {
		
		File file = new File(mapperFile);
		FileInputStream in = null;
		Properties keyMapper = new Properties();
		if (file.exists()) {
			in = new FileInputStream(mapperFile);
			keyMapper.load(in);
		}
		keyMapper.setProperty(key, Long.toString(Metalocation) + "-" + Integer.toString(Metalength) +  "-" + Long.toString(contentLocation) + "-" + Integer.toString(contentLength));
		FileOutputStream out = new FileOutputStream(mapperFile);
		keyMapper.store(out, null);
		out.close();
		if (in != null) {
			in.close();
		}
	}
	
	private long getContentLocation(String filename) throws IOException {
		String value = getFileProperties(filename);
		String [] values = value.split("-");
		
		return Long.parseLong(values[2]);
		
	}
	
	private int getContentLength(String filename) throws IOException {
		String value = getFileProperties(filename);
		String [] values = value.split("-");
		return Integer.parseInt(values[3]);
		
	}
	
	private long getMetaLocation(String filename) throws IOException {
		String value = getFileProperties(filename);
		String [] values = value.split("-");
		
		return Long.parseLong(values[0]);
		
	}
	
	private int getMetaLength(String filename) throws IOException {
		String value = getFileProperties(filename);
		String [] values = value.split("-");
		return Integer.parseInt(values[1]);
		
	}
	
	private String getFileProperties(String key) throws IOException {
    	String value = null;
		Properties keyMapper = new Properties();
		FileInputStream in = new FileInputStream(mapperFile);
		keyMapper.load(in);

		if (keyMapper.getProperty(key) != null) {
			value = keyMapper.getProperty(key);
		} 
		in.close();
		return value;
	}
}
