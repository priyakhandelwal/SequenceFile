//package com.sample.sequence;
//
//import java.io.IOException;
//import java.util.EnumSet;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.CreateFlag;
//import org.apache.hadoop.fs.FSDataInputStream;
//import org.apache.hadoop.fs.FileContext;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.SequenceFile;
//import org.apache.hadoop.io.SequenceFile.CompressionType;
//import org.apache.hadoop.io.SequenceFile.Metadata;
//import org.apache.hadoop.io.SequenceFile.Writer;
//import org.apache.hadoop.io.Text;
//
//
//
//public class AppendSequenceFile {
//
//    /**
//     * @param args
//     * @throws IOException
//     * @throws IllegalAccessException
//     * @throws InstantiationException
//     */
//    public static void main(String[] args) throws IOException,
//            InstantiationException, IllegalAccessException {
//
//        Configuration conf = new Configuration();
////        conf.set("io.serializations",
////                "org.apache.hadoop.io.serializer.JavaSerialization");
////            conf.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
//        FileSystem fs = FileSystem.get(conf);
//        Path inputFile = new Path("/Users/prkhandelwal/Desktop/Sample/SequenceFile/sampleAppendTextFiles");
//        Path sequenceFile = new Path("/Users/prkhandelwal/Desktop/Sample/SequenceFile/samplop");
//        FSDataInputStream inputStream;
//        
//        SequenceFile.Metadata metadata = new SequenceFile.Metadata();
//        metadata.set(new Text("Key1"), new Text("value1"));
//        Writer.Option metadataOption = Writer.metadata(metadata);
//        
//        Text key = new Text();
//        Text value = new Text();
//
//// Writer writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(sequenceFile), SequenceFile.Writer.keyClass(Text.class), SequenceFile.Writer.valueClass(Text.class), SequenceFile.Writer.appendIfExists(true), metadataOption);
//        SequenceFile.Writer writer = SequenceFile.createWriter(FileContext.getFileContext(conf), conf, sequenceFile, Text.class, Text.class, CompressionType.NONE, null, new Metadata(), EnumSet.of(CreateFlag.CREATE, CreateFlag.APPEND));
////        FileStatus[] fStatus = fs.listStatus(inputFile);
////        String str = "";
////        for (FileStatus fst : fStatus) {
////            str = "";
////            System.out.println("Processing file : " + fst.getPath().getName() + " and the size is : " + fst.getPath().getName().length());
////            inputStream = fs.open(fst.getPath());
////            key.set(fst.getPath().getName());
////            while(inputStream.available()>0) {
////                str = str+inputStream.readLine();
////            }
////        }
////        value.set(str);
////        writer.append(key, value);
//        writer.append(new Text("yoo"), new Text("kihai"));
//        writer.close();
////        	writer.append(new Text("HEllo"), new Text("Birdie"));
////        	writer.close();
//    }
//}