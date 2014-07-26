package helloworld;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsUtils {
	public static String read(FileSystem fs, Path path) throws IOException {
		InputStream in = fs.open(path);
		try {
			return IOUtils.toString(in);
		} finally {
			in.close();
		}
	}
	
	
}
