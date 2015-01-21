package edu.upenn.cis455.mapreduce.job;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;

public class Init implements Job {
	public void map(String key, String value, Context context) {
		//String edge = value.toString();
		// separate key and value
		//String[] vertices = edge.split("\\t");
		String out = value.trim() + ",";
			// write as Text V[0] Text v[1],r1 separated by commas.
			
		context.write(key, out);
	}

	public void reduce(String key, String[] values, Context context) {

		String edge = "";
		// used to count number of friends
		int count = 0;
		String combine = "";
		for (String val : values) {
			count++;
			// combine all vertices. get form vi = vj, vj+1,...vj+n
			combine = combine + val.toString();
		}
		// add an initial rank of 1.0
		combine = combine + "rank=1.0,";
		// add the count to the end
		combine = combine + "count=" + count;

		// emit vertex as key and friends plus count and rank as values
		// separated by commas
		System.out.println("init");
		System.out.println("key:"+key+"    val:"+combine);

		context.write(key, combine);
	}
}
