package squencefile;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class BundleSeq {

    /**
     * @param args
     * @throws IOException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public static void main(String[] args) throws IOException,
            InstantiationException, IllegalAccessException {
        // TODO Auto-generated method stub

        Configuration conf = new Configuration();
//        conf.addResource(new Path(
//                "/hadoop/projects/hadoop-1.0.4/conf/core-site.xml"));
//        conf.addResource(new Path(
//                "/hadoop/projects/hadoop-1.0.4/conf/hdfs-site.xml"));
        FileSystem fs = FileSystem.get(conf);
        Path inputFile = new Path("/Users/prkhandelwal/Desktop/Sample/SequenceFile/sampleTextFiles");
        Path outputFile = new Path("/Users/prkhandelwal/Desktop/Sample/SequenceFile/outputfile");
        FSDataInputStream inputStream;
        Text key = new Text();
        Text value = new Text();
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf,
                outputFile, key.getClass(), value.getClass());
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
            writer.append(key, value);

        }
        fs.close();
        IOUtils.closeStream(writer);
        System.out.println("SEQUENCE FILE CREATED SUCCESSFULLY........");
    }
}