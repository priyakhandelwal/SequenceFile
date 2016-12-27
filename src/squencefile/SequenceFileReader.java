package squencefile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
//import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;

public class SequenceFileReader{
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("/Users/prkhandelwal/Desktop/Sample/SequenceFile/outputfile");
		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(fs, path, conf);
			Text key = new Text();
			Text value = new Text();
			while (reader.next(key, value)) { System.out.println(key);
			System.out.println(value);
			System.out.println("------------------------------");
//				reader.getCurrentValue(key);
			}
		} finally {
			IOUtils.closeStream(reader);
		}
	}
}