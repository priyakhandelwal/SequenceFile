package com.sample.sequence.utils;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

public class LocationMapper {
	private String mapperFile = "/Users/prkhandelwal/Desktop/Sample/SequenceFile/key.properties";
	private Properties keyMapper = new Properties();
	
	public long getKeyLocation(String key) throws IOException {
		long location = -1;
		FileInputStream in = new FileInputStream(mapperFile);
		keyMapper.load(in);
		System.out.println("key = " + key);
		if (keyMapper.getProperty(key) != null) {
			location = Long.parseLong(keyMapper.getProperty(key));
			System.out.println("location = " + location);
			System.out.println(location);
		} 
		
		return location;
	}
	
	public void saveLocation(String key, long location) throws IOException {
		keyMapper.setProperty(key, Long.toString(location));
		FileOutputStream out = new FileOutputStream(mapperFile);
		keyMapper.store(out, null);
		out.close();
	}
}
