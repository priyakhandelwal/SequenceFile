package com.paypal.storageImpl;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class StorageImplTest {
	private StorageImpl storage;
	private Map<String, String> metadata;
	private byte[] meta;
	private String filename = "perf6001.jpeg";
	private String mockContent = "Try and test";
	
	@Before
	public void setupForEachTest() throws IOException {
		storage = new StorageImpl();
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
	
	@Ignore
	@Test
	public void getMetaDataTest() throws IOException {
		assertEquals(true, Arrays.equals(meta, storage.getMetadata(filename)));
	}
	
	@Ignore
	@Test
	public void createTestPerformance() throws IOException, InterruptedException {
		byte[] meta = new byte[100000];
		new Random().nextBytes(meta);
		byte[] mockdata = new byte[100000];
		new Random().nextBytes(mockdata);
		
		Long startTime = System.currentTimeMillis();
		for (int i=5001; i<6000; i++) {
			filename = "perf" + i + ".jpeg";
			storage.create(filename, mockdata, metadata);
		}
		Long endTime = System.currentTimeMillis();
		System.out.println(endTime-startTime);
		
	}
	
	@Ignore
	@Test
	public void readTestPerformance() throws IOException {
		Long startTime = System.currentTimeMillis();
		for (int i=5001; i<6000; i++) {
			filename = "perf" + i + ".jpeg";
			storage.read(filename);
		}
		Long endTime = System.currentTimeMillis();
		System.out.println(endTime-startTime);
	}
}
