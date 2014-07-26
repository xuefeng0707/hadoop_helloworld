package helloworld;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class WordCountTest {

	private static class WordCountMapper extends MapReduceBase implements
			Mapper<Object, Text, Text, IntWritable> {

		private static final IntWritable ONE = new IntWritable(1);

		private Text word = new Text();

		public void map(Object key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			StringTokenizer tokenizer = new StringTokenizer(value.toString());
			String token;
			while (tokenizer.hasMoreTokens()) {
				token = tokenizer.nextToken();
				word.set(token);
				output.collect(word, ONE);
			}
		}
	}

	private static class WordCountReducer extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {
		
		private IntWritable count = new IntWritable(0);

		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			while(values.hasNext()) {
				sum += values.next().get();
			}
			count.set(sum);
			
			output.collect(key, count);
		}

	}

	public static void main(String[] args) throws IOException {
		JobConf conf = new JobConf(WordCountTest.class);
		
		// next line needed while running from Eclipse
		conf.setJar("/home/test/work/env/hadoop/hadoop-0.20.2/wordcount/wordcount.jar");
		conf.setJobName("WordCount");
		
//		String input = "hdfs://192.168.1.9:9000/user/test/wordcount";
//		String output = "hdfs://192.168.1.9:9000/user/test/wordcount/result";
//		
//		conf.addResource("classpath:/hadoop/core-site.xml");
//		conf.addResource("classpath:/hadoop/hdfs-site.xml");
//		conf.addResource("classpath:/hadoop/mapred-site.xml");
		
		// hadoop-0.20.2
		String input = "hdfs://test:9000/usr/hadoop_0_20_2/wordcount";
		String output = "hdfs://test:9000/usr/hadoop_0_20_2/wordcount/result";
		
		conf.addResource("hadoop-0.20.2/core-site.xml");
		conf.addResource("hadoop-0.20.2/hdfs-site.xml");
		conf.addResource("hadoop-0.20.2/mapred-site.xml");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setMapperClass(WordCountMapper.class);
		conf.setCombinerClass(WordCountReducer.class);
		conf.setReducerClass(WordCountReducer.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(output), true);
		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));
		
		JobClient.runJob(conf);
		
		// output
		for(FileStatus f : fs.listStatus(new Path(output))) {
			if(!f.isDir()) {
				System.out.println(f.getPath() + ":");
				System.out.println(HdfsUtils.read(fs, f.getPath()));
			}
		}
		System.out.println();
		System.exit(0);
	}

}
