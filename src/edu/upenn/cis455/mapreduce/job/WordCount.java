package edu.upenn.cis455.mapreduce.job;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;

public class WordCount implements Job {

  public void map(String key, String value, Context context)
  {
	  //give a count for all words
	  String[] words = value.split(" ");
	  for (String w : words) {
		  w = w.replaceAll("\\s+", "");
		  if (!w.equals("")) {
			  context.write(w,"1");
		  }
	  }
  }
  
  public void reduce(String key, String[] values, Context context)
  {
	
	  int count = 0;
	  for (String s : values) {
		  count++;
	  }
	  context.write(key, String.valueOf(count));
    // Your reduce function for WordCount goes here
  }
  
}
