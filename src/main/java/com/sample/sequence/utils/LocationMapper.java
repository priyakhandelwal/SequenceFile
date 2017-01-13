package com.sample.sequence.utils;

import java.io.File;
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

		if (keyMapper.getProperty(key) != null) {
			location = Long.parseLong(keyMapper.getProperty(key));
		} 
		in.close();
		return location;
	}
	
	public void saveLocation(String key, long location) throws IOException {
		File file = new File(mapperFile);
		FileInputStream in = null;
		if (file.exists()) {
			in = new FileInputStream(mapperFile);
			keyMapper.load(in);
		}
		keyMapper.setProperty(key, Long.toString(location));
		FileOutputStream out = new FileOutputStream(mapperFile);
		keyMapper.store(out, null);
		out.close();
		if (in != null) {
			in.close();
		}
	}
}
