package squencefile;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;

public class GetFiles {
	public static void main(String args[]) throws Exception{
		String uri = args[0];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path path = new Path(uri);
		
		FileStatus status[] = fs.listStatus(path);
		Path[] paths = FileUtil.stat2Paths(status);
		for (Path p: paths) {
			System.out.println(p.toString());
		}
	}
}
