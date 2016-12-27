package squencefile;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class AppendSequenceFile {

    /**
     * @param args
     * @throws IOException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public static void main(String[] args) throws IOException,
            InstantiationException, IllegalAccessException {

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);
        Path inputFile = new Path("/Users/prkhandelwal/Desktop/Sample/SequenceFile/sampleAppendTextFiles");
        Path sequenceFile = new Path("/Users/prkhandelwal/Desktop/Sample/SequenceFile/outputfile");
        FSDataInputStream inputStream;
        Text key = new Text();
        Text value = new Text();
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf,
        		sequenceFile, key.getClass(), value.getClass());
        FileStatus[] fStatus = fs.listStatus(inputFile);

        for (FileStatus fst : fStatus) {
            String str = "";
            System.out.println("Processing file : " + fst.getPath().getName() + " and the size is : " + fst.getPath().getName().length());
            inputStream = fs.open(fst.getPath());
            key.set(fst.getPath().getName());
            while(inputStream.available()>0) {
                str = str+inputStream.readLine();
            }
            value.set(str);
            System.out.println("Value is = " + value);
            writer.append(key, value);

        }
//        String str = "";
//        inputStream = new FileInputStream("/Users/prkhandelwal/Desktop/Sample/SequenceFile/sampleTextFiles/temporary.txt");
//        key.set("temporary.txt");
//        while(inputStream.available()>0) {
//        	str = str+inputStream.read();
//        }
//        System.out.println("value  = " + str);
//        value.set(str);
//        inputStream.close();
//        fs.close();
//        IOUtils.closeStream(writer);
//        System.out.println(" FILE APPENED SUCCESSFULLY........");
    }
}