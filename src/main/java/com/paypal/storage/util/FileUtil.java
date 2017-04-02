package com.paypal.storage.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Properties;

public class FileUtil {
	private static FileUtil instance;
	private static final String MAPPER_FILE = "/Users/prkhandelwal/Desktop/Sample/Dat-storage/storage-mapper-test1.properties";
	
	private FileInputStream fileInputStream;
	
	private RandomAccessFile file;
	private Properties keyMapper = new Properties();
	private FileOutputStream fileOutputStream;

	private FileUtil(String PATH) {
		try {
			file = new RandomAccessFile(PATH, "rw");
			if ((new File(MAPPER_FILE)).exists()) {
				fileInputStream = new FileInputStream(MAPPER_FILE);
				keyMapper.load(fileInputStream);
			}
			
			fileOutputStream = new FileOutputStream(MAPPER_FILE, true);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static FileUtil getInstance(String PATH) {
		if (instance == null) {
			synchronized(FileUtil.class) {
				if (instance == null) {
					instance = new FileUtil(PATH);
				}
			}
		}
		return instance;
	}
	
	public RandomAccessFile getFile() {
		return file;
	}


	public FileOutputStream getFileOutputStream() {
		return fileOutputStream;
	}

	public Properties getKeyMapper() {
		return keyMapper;
	}

	
}
