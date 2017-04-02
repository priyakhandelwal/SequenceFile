package com.paypal.storageImpl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;

import com.paypal.storage.Storage;
import com.paypal.storage.util.FileUtil;

public class StorageImpl implements Storage{
	private static final String PATH = "/Users/prkhandelwal/Desktop/Sample/Dat-output/storage-test1.dat";
	
	private final FileUtil fileUtil = FileUtil.getInstance(PATH);
	
	public boolean create(String filename, byte[] content, Map<String, String> metadata) {
		boolean isCreated = false;
		byte[] meta;
		
		try {
			meta = convertMetadataInBytes(metadata);
			long metalocation = fileUtil.getFile().length();
			long contentLocation = fileUtil.getFile().length() + meta.length;
			
			isCreated = writeToFile(filename, meta, content);
			
			if (isCreated) {
				saveMetaAndContentsLengthAndLocation(filename, metalocation, meta.length, contentLocation, content.length);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return isCreated;
	}
	
	public byte[] read(String filename) {
		long contentLocation;
		int contentSize;
		byte [] content = null;

		try {
			contentLocation = Long.parseLong(getMetaAndContentsLengthAndLocation(filename)[2]);
			contentSize = Integer.parseInt(getMetaAndContentsLengthAndLocation(filename)[3]);
			content = new byte[contentSize];
			fileUtil.getFile().seek(contentLocation);
			fileUtil.getFile().read(content);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		return content;
		
	}
	
	public byte[] getMetadata(String filename) {
		long metaLocation;
		int metaSize;
		byte [] metadata = null;
		
		try {
			metaLocation = Long.parseLong(getMetaAndContentsLengthAndLocation(filename)[0]);
			metaSize = Integer.parseInt(getMetaAndContentsLengthAndLocation(filename)[1]);
			metadata = new byte[metaSize];
			fileUtil.getFile().seek(metaLocation);
			fileUtil.getFile().read(metadata);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return metadata;
		
	}
	
	private boolean writeToFile(String filename, byte[] metadata, byte[] content) throws IOException {
		fileUtil.getFile().seek(fileUtil.getFile().length());
		fileUtil.getFile().write(metadata);
		fileUtil.getFile().write(content);

		return true;
	}

	private byte[] convertMetadataInBytes(Map<String, String> metadata) throws IOException {
		ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
		ObjectOutputStream out = new ObjectOutputStream(byteOut);
		out.writeObject(metadata);
		out.close();
		return byteOut.toByteArray();
	}
	
	private void saveMetaAndContentsLengthAndLocation(String key, long Metalocation, int Metalength, long contentLocation, int contentLength) throws IOException {
		fileUtil.getKeyMapper().setProperty(key, Long.toString(Metalocation) + "-" + Integer.toString(Metalength) +  "-" + Long.toString(contentLocation) + "-" + Integer.toString(contentLength));
		fileUtil.getKeyMapper().store(fileUtil.getFileOutputStream(), null);
	}
	

	private String[] getMetaAndContentsLengthAndLocation(String key) throws IOException {
    	String value = null;
    	String [] metaAndContent = new String[4];
		if (fileUtil.getKeyMapper().getProperty(key) != null) {
			value = fileUtil.getKeyMapper().getProperty(key);
		} 
		metaAndContent = value.split("-");
		return metaAndContent;
	}
}
