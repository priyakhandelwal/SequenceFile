package squencefile;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class TextToSequenceConverter {
    /**
     * @param args
     * @throws IOException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws IOException,
            InstantiationException, IllegalAccessException {
        // TODO Auto-generated method stub

        Configuration conf = new Configuration();
//        conf.addResource(new Path("/hadoop/projects/hadoop-1.0.4/conf/core-site.xml"));
//        conf.addResource(new Path("/hadoop/projects/hadoop-1.0.4/conf/hdfs-site.xml"));
        FileSystem fs = FileSystem.get(conf);
        Path inputFile = new Path("/Users/prkhandelwal/Desktop/Sample/SequenceFile/sampleTextFiles");
        FSDataInputStream inputStream = fs.open(inputFile);
        Path outputFile = new Path("/Users/prkhandelwal/Desktop/Sample/SequenceFile/");
        IntWritable key = new IntWritable();
        int count = 0;
        Text value = new Text();    
        String str;
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf,outputFile, key.getClass(), value.getClass());
        while (inputStream.available() > 0) {
            key.set(count++);
            str = inputStream.readLine();
            value.set(str);
            writer.append(key, value);
        }
        fs.close();
        IOUtils.closeStream(writer);
        System.out.println("SEQUENCE FILE CREATED SUCCESSFULLY........");
    }
}