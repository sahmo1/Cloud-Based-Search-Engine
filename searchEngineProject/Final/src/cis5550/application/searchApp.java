package cis5550.application;

import static cis5550.webserver.Server.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.nio.file.Files;


import cis5550.ranker.Ranker;
import cis5550.generic.Master;
import cis5550.kvs.KVSClient;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;


public class searchApp extends cis5550.generic.Master {
	
	public static KVSClient kvs;

	public static void main(String[] args) throws Exception {
		port(8080);
		// Get current directory
		String currentDirectory = Paths.get(".").toAbsolutePath().normalize().toString();
		String staticFile = currentDirectory + "/Final/src/cis5550/frontend/index.html";
		String staticJSFile = currentDirectory + "/Final/src/cis5550/frontend/main.js";
		
		get("/", (req, res) -> {
			res.type("text/html");
			res.header("Access-Control-Allow-Origin", "*");
			res.header("Access-Control-Allow-Methods", "GET");
			res.header("Access-Control-Allow-Headers", "*");
			res.header("Access-Control-Allow-Credentials", "true");
			res.header("Access-Control-Max-Age", "3600");

			byte[] fileContent = Files.readAllBytes(Paths.get(staticFile));
            return new String(fileContent);
		});

		get("/main.js", (req, res) -> {
			res.type("text/html");
			res.header("Access-Control-Allow-Origin", "*");
			res.header("Access-Control-Allow-Methods", "GET");
			res.header("Access-Control-Allow-Headers", "*");
			res.header("Access-Control-Allow-Credentials", "true");
			res.header("Access-Control-Max-Age", "3600");

			byte[] fileContent = Files.readAllBytes(Paths.get(staticJSFile));
            return new String(fileContent);
		});
	
		//post request allows query to be sent in body of request
		post("/search", (req,res) -> 
		{ 	

			//remove CORS restriction for local testing
			res.header("Access-Control-Allow-Origin", "*");
			res.header("Access-Control-Allow-Methods", "POST");
			res.header("Access-Control-Allow-Headers", "*");
			res.header("Access-Control-Allow-Credentials", "true");
			res.header("Access-Control-Max-Age", "3600");
			long startTime = System.currentTimeMillis(); 
			kvs = new KVSClient(args[0]);

			//String output = "";

			String query = req.body(); //query={this is the query}

			//use URLDecoder to decode the query
			query = URLDecoder.decode(query.split("=")[1], "UTF-8");
			
			//System.out.println("query: " + query);

			String page = req.queryParams("page");
			if (page == null) {
				page = "1";
			}

			
			//remove the "query=" from the beginning of the string
			//query = query.substring(6);

			// if + is in the query, replace with space
			
			//query = query.replace("+", " ");
			
			Ranker ranker = new Ranker(query, page, kvs);

			Map<String, Object> urls = ranker.getResults(page);

			System.out.println("Search finished for this query: " + ranker.queryRaw);

			long endTime = System.currentTimeMillis(); 
			long queryProcessingDurationMS = endTime - startTime;
			long queryProcessingDurationS = TimeUnit.MILLISECONDS.toSeconds(endTime - startTime);  
			if (queryProcessingDurationS == 0) {
				System.out.println("Processing duration for this query (in milliseconds): " + Long.toString(queryProcessingDurationMS));
			} else {
				System.out.println("Processing duration for this query (in seconds):" + Long.toString(queryProcessingDurationS));
			}
		
			res.type("application/json");
			res.status(200 , "OK");

			String jsonResponse = convertMapToJson(urls);

			res.write(jsonResponse.getBytes());

			
			return "OK";

			
		}); 	    
		
	}

	private static String convertMapToJson(Map<String, Object> map) {
		StringBuilder json = new StringBuilder("{");
	
		Iterator<Map.Entry<String, Object>> iterator = map.entrySet().iterator();
	
		while (iterator.hasNext()) {
			Map.Entry<String, Object> entry = iterator.next();
			String key = entry.getKey();
			Object value = entry.getValue();
	
			json.append("\"").append(key).append("\":");
	
			if (value instanceof String) {
				json.append("\"").append(value).append("\"");
			} else if (value instanceof List) {
				List<String> values = (List<String>) value;
				json.append("[");
				for (int i = 0; i < values.size(); i++) {
					json.append("\"").append(values.get(i)).append("\"");
					if (i < values.size() - 1) {
						json.append(",");
					}
				}
				json.append("]");
			} else {
				json.append(value);
			}
	
			if (iterator.hasNext()) {
				json.append(",");
			}
		}
	
		json.append("}");
		return json.toString();
	}

}