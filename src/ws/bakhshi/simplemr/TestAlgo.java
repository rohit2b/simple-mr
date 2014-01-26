package ws.bakhshi.simplemr;

import org.apache.hadoop.io.Text;

public class TestAlgo {

	public static void main(String[] args) {
		
		// Set test data here
		
		Text value = new Text("the line the word the");
		
		// ----- Set over -----------
		
		String line = value.toString();
		int count = 0;
		
		// Split the input line into words, separated by a space ' ' 
		String[] words = line.split(" ");
		
		System.out.println("# of words: " + words.length);
		
		for( int i=0; i< words.length; i++)
		{
			if(words[i].equals("the"))
				count++;
		}
		
		System.out.println("The count is: " + count);

	}

}
