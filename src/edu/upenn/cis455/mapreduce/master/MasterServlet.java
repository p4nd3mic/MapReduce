package edu.upenn.cis455.mapreduce.master;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.*;
import javax.servlet.http.*;

public class MasterServlet extends HttpServlet {

	static final long serialVersionUID = 455555001;
	private Map<String, Map<String, String>> workers = new HashMap<String, Map<String, String>>();
	private String test;
	private boolean waiting = false;
	private boolean iter = true;
	private boolean init = true;
	private boolean diff1 = true;
	private boolean diff2 = true;
	private boolean diff3 = true;
	private boolean finish = false;
	private boolean diffReceived = false;
	private int evenOdd = 0;
	private boolean multipart = false;
	private String answer; 
	private Double diff = 1000.0;
	
	public void doGet(HttpServletRequest request, HttpServletResponse response)
			throws java.io.IOException {
		String path = request.getServletPath();
		if(path == null) {
			System.out.println("path is null///////////////////////////");
		}
		if (path.equals("/status")) {
			System.out.println("in status");
			PrintWriter out = response.getWriter();
			response.setContentType("text/html");
			masterStatus(out);
		} else if (path.equals("/workerstatus")) {
			System.out.println("worker status-----------");

			String query = request.getQueryString();
			String ip = request.getRemoteAddr();
			workerStatus(query, ip);
		}	else if (path.equals("/diff")) {
			//diff from worker
			diffReceived = true;
			String diff = request.getParameter("diff");
			this.diff = Double.parseDouble(diff);
			System.out.println(this.diff);
		}
	}



	// logic when worker sends get with info about worker status.
	// will still show the same page at the end.
	public void workerStatus(String query, String workerIP) {
		// need to break up query string by parameters.
		if (query == null) {
			System.out.println("query == null");
		} else {
			String[] parameters = query.split("&");
			Map<String, String> queryMap = parseQuery(parameters);
			queryMap.put("ip", workerIP);
			Date date = new Date();
			queryMap.put("time", date.toString());

			//handle cases where no keys have been written or read.
			if (!queryMap.containsKey("keysread")) {
				queryMap.put("keysread", "0");
			}
			if (!queryMap.containsKey("keyswritten")) {
				queryMap.put("keyswritten", "0");
			}
			// save worker in HashMap of workers by IP.
			workers.put(workerIP+":"+queryMap.get("port"), queryMap);
			checkWaiting();
			boolean completed = checkCompleted();
			if (multipart && completed) {
				composite();
			}
		}
	}
	public void composite() {
		if (init) {
			init = false;
			System.out.println("run init----------------");
			runJob("Init","input","interm1");
		} else if (iter && evenOdd % 2 ==0) {
			evenOdd++;
			runJob("Iter","interm1","interm2");
		} else if (iter && evenOdd % 2 != 0) {
			
			iter = false;
			evenOdd++;
			runJob("Iter","interm2","interm1");
		} else if (diff1) {
			//function to combine files of interm1 and interm2
			System.out.println("in diff1");
			diff1 = false;
			runJob("Diff1","diff","DiffInterm");
		} else if (diff2 ) {
			System.out.println("in diff2");
			diff2 = false;
			runJob("Diff2","DiffInterm","diffOutput");
		} else if (diffReceived) {
			 if (diff < .001) {
				System.out.println("convergence reached");
				 finish = true;
				 diffReceived = false;
			 } else {
				 System.out.println("diff ="+diff);
				 iter = true;
				 diff1 = true; 
				 diff2 = true;
				 //diffReceived = false;
			 }
		} else if (finish) {
			finish = false;
			System.out.println("running finish");
			  if (evenOdd % 2 == 0) {
				  runJob("Finish","interm1", "output");
			  } else {
				  runJob("Finish","interm2", "output");
			  }		
		}

	}

	public void runJob(String mr, String input, String output) {
		//will set class. and call runmap();
		String job = "job=edu.upenn.cis455.mapreduce.job."+mr;
		input = "&input="+input;
		output = "&output="+output;
		String threads = "&map=3&reduce=3";
		answer = job + input + output + threads;
		System.out.println("answer="+answer);

		try {
			runMap(answer);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public boolean checkCompleted() {
		boolean completed = true;
		for (String key : workers.keySet()) {
			Map<String,String> map =  workers.get(key);
			if (!map.get("status").equals("idle")) {
				completed = false;
				break;
			}
		}
		return completed;
		
	}
	public void checkWaiting() {
		boolean waiting = true;
		for (String key : workers.keySet()) {
			Map<String,String> map =  workers.get(key);
			if (!map.get("status").equals("waiting")) {
				waiting = false;
				break;
			}
		}
		
		if (waiting) {
			// send /runreduce to all workers.
			runReduce();
		}
	}
	public void runReduce() {
				String strWorkers = "";
				for (String name : workers.keySet()) {
					strWorkers += name+";";
				}
				for (String name : workers.keySet()) {
					String entireURL = "http://" + name + "/runreduce";
					URL url;
					
					try {
						url = new URL(entireURL);
						HttpURLConnection http = (HttpURLConnection) url.openConnection();
						http.setRequestMethod("POST");
						http.setRequestProperty("User-Agent", "Mozilla/5.0");

						http.setDoOutput(true);
						http.setUseCaches(false);
						DataOutputStream wr = new DataOutputStream(http.getOutputStream());
						
						wr.writeBytes("&job=asdf");
						wr.flush();
						wr.close();
						
						BufferedReader in1 = new BufferedReader(new InputStreamReader(http.getInputStream()));
						String line1;
						StringBuffer answer1 = new StringBuffer();
						while ((line1 = in1.readLine())!= null) {
							answer1.append(line1);
						}

					} catch (MalformedURLException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				
	}
	public Map<String, String> parseQuery(String[] parameters) {
		Map<String, String> queryMap = new HashMap<String, String>();
		for (String p : parameters) {
			String[] nameField = p.split("=");
			queryMap.put(nameField[0].toLowerCase(), nameField[1]);
		}
		return queryMap;
	}

	public void masterStatus(PrintWriter out) {
		workerTable(out);
	}
	
	public void workerTable(PrintWriter out) {
		String port, ip, status,job,keysRead,keysWritten;
		String worker = "";
		String table = "<!DOCTYPE html><html><head><style>table,th,td" +
				"{border:1px solid black;border-collapse:collapse;}" +
				"th,td{padding:5px;}" +
				"</style></head><body>" +
				"<table style=\"width:300px\">" +
				"<tr>" +
					"<th>IP:Port</th>" +
					"<th>Status</th>" +		
					"<th>Job</th>" +
					"<th>Keys Read</th>" +
					"<th>KeysWritten</th>" +
				"</tr><tr>";
		for (String name : workers.keySet()) {
			Map<String,String> workerValues = workers.get(name);
			port = workerValues.get("port");
			ip = workerValues.get("ip");
			status = workerValues.get("status");
			job = workerValues.get("job");
			keysRead = workerValues.get("keysread");
			keysWritten = workerValues.get("keyswritten");
			worker += "<tr>" +
					"<td>"+ip+":"+port+"</td>" +
					"<td>"+status+"</td>" +		
					"<td>"+job+"</td>" +
					"<td>"+keysRead+"</td>" +
					"<td>"+keysWritten+"</td>" +
				"</tr>";
			}				
		table += worker;	
		table += "</table>Josh Willis (jmwillis)</body></html>";
		out.println(table);
		
		String form = "<br><br><form name=\"input\" action=\"/status\" method=\"post\">"+
		"<br>JobClassName: <input type=\"text\" name=\"job\">"+
		"<br>InputDirectory: <input type=\"text\" name=\"input\">"+
		"<br>OutputDirectory: <input type=\"text\" name=\"output\">"+
		"<br>MapThreads: <input type=\"text\" name=\"map\">"+
		"<br>ReduceThreads: <input type=\"text\" name=\"reduce\">"+

		"<br><input type=\"submit\" value=\"Submit\">"+
		"</form>";
		out.println(form);
	}
	
	public void doPost(HttpServletRequest request, HttpServletResponse response)
			throws java.io.IOException {
		//ignore if multipart
		if (!multipart){
			BufferedReader in = request.getReader();
			String line;
			StringBuffer answer = new StringBuffer();
			while ((line = in.readLine())!= null) {
				answer.append(line);
			}
			System.out.println(answer);
			in.close();
			runMap(answer.toString());
		}
	}
	public void runMap(String answer) throws IOException {

		String strWorkers = "";
		for (String name : workers.keySet()) {
			strWorkers += name+";";
		}
		for (String name : workers.keySet()) {
			String entireURL = "http://" + name + "/runmap";
			URL url;
			
			try {
				url = new URL(entireURL);
				HttpURLConnection http = (HttpURLConnection) url.openConnection();
				http.setRequestMethod("POST");
				http.setRequestProperty("User-Agent", "Mozilla/5.0");

				http.setDoOutput(true);
				http.setUseCaches(false);
				DataOutputStream wr = new DataOutputStream(http.getOutputStream());
				wr.writeBytes(answer.toString()+"&workers="+strWorkers);
				wr.flush();
				wr.close();
				
				BufferedReader in1 = new BufferedReader(new InputStreamReader(http.getInputStream()));
				String line1;
				StringBuffer answer1 = new StringBuffer();
				while ((line1 = in1.readLine())!= null) {
					answer1.append(line1);
				}
				in1.close();
				
			} catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}
	
	
}
