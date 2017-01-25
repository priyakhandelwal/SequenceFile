package com.paypal.storage;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

public interface Storage {
	public boolean create(String filename, byte[] content, Map<String, String> metadata) throws IOException;
	public byte[] read(String filename) throws FileNotFoundException;
	public byte[] getMetadata(String filename);
}
