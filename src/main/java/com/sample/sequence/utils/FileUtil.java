package com.sample.sequence.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class FileUtil {
	public void writeToFile(String result, String filename) {
		String filePath = "/Users/prkhandelwal/Desktop/Sample/SequenceFile/storage_output";
		BufferedWriter bw = null;
		FileWriter fw = null;
		
		try {
			fw = new FileWriter(filePath + "/" + filename);
			bw = new BufferedWriter(fw);
			bw.write(result);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
				try {
					if (bw != null) {
						bw.close();
					}
					if (fw != null) {
						fw.close();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}	
		}
	}
	
	public long getByteOffset(String filePath) {
		File file = new File(filePath);
		
		long bytesize = -1;
		if (file.exists()) {
			bytesize = file.length();
		} 
		return bytesize;
	}
}
