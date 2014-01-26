package ws.bakhshi.simplemr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class SimpleMapper extends MapReduceBase
	implements Mapper<LongWritable, Text, Text, IntWritable>{

	/**
	 * Search through each line and count the number of 'the' instances.
	 */
	public void map(LongWritable key, Text value, 
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		
		// Parameters:
		// key - key of the row
		// value - one row's text read by map
		// output - output of map
		// reporter - 
		
		String line = value.toString();
		int count = 0;
		
		// Split the input line into words, separated by a space ' ' 
		String[] words = line.split(" ");
		
		for( int i=0; i< words.length; i++)
		{
			if(words[i].equals("the"))
				count++;
		}
		
		// Write to map output
		output.collect(new Text("the"), new IntWritable(count));
	}
}
