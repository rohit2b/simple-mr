package ws.bakhshi.simplemr;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileInputFormat;

/**
 * Main program to run the MapReduce job
 * 
 * @author rohit
 *
 */
public class SimpleMapJob {

	public static void main(String[] args) {
		
		JobClient client = new JobClient();
		
		// This will hold the configs for this job
		JobConf conf = new JobConf(ws.bakhshi.simplemr.SimpleMapJob.class);
		
		// Define name od job
		conf.setJobName("Simple MR job");
		
		// Set data output type - Text for key, Int writable for value
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		// Set the mapper class
		conf.setMapperClass(ws.bakhshi.simplemr.SimpleMapper.class);
		
		// Don't use a reducer
		conf.setNumReduceTasks(0);
		
		// Set the file input and output paths
		// FileInputFormat is the base class for all file-based InputFormats
		// Ref: https://hadoop.apache.org/docs/stable2/api/org/apache/hadoop/mapred/FileInputFormat.html
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		client.setConf(conf);
		
		try{
			// Run the job
			JobClient.runJob(conf);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

}
