package com.paypal.storageImpl;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class StorageImplTest {
	private StorageImpl storage;
	private Map<String, String> metadata;
	private byte[] meta;
	private String filename;
	private String mockContent;
	
	@Before
	public void setupForEachTest() throws IOException {
		storage = new StorageImpl();
		metadata = new HashMap<String, String>();
		filename = "test5.jpeg";
		metadata.put("filename", filename);
		ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
		ObjectOutputStream out = new ObjectOutputStream(byteOut);
		out.writeObject(metadata);
		out.close();
		meta = byteOut.toByteArray();
		mockContent = "randomarraybytesfortestingtest2array";
	}
	
	@Ignore
	@Test
	public void createTest() throws IOException {
		assertEquals(true, storage.create(filename, mockContent.getBytes(), metadata));
	}
	
	@Ignore
	@Test
	public void readTest() throws FileNotFoundException {
		assertEquals(true, Arrays.equals(mockContent.getBytes(), storage.read(filename)));
	}
	
//	@Ignore
	@Test
	public void getMetaDataTest() throws IOException {
		assertEquals(true, Arrays.equals(meta, storage.getMetadata(filename)));
	}
}
