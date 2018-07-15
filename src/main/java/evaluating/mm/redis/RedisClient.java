package evaluating.mm.redis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.redislabs.modules.rejson.JReJSON;
import com.redislabs.modules.rejson.Path;

//import evaluating.mm.redis.utils.Commands;

public class RedisClient {
	
	private static Jedis jedis;
	
	private static Map<String, Map<String,String>> redis;
	
	public static void init () {
		redis = new HashMap<>();
		jedis = new Jedis("127.0.0.1", Protocol.DEFAULT_PORT, 60000);
		jedis.connect();
	}
	
	public static void read () {
		Set<String> keys = jedis.keys("v*");
		Iterator<String> it = keys.iterator();
		//Map<String, String> values = new HashMap<>();
		while (it.hasNext()) {
			String key= it.next();
			
			if (key.startsWith("v")||key.startsWith("e")){
				redis.put(key, jedis.hgetAll(key));
			}
		}
	}
	
	public static void readE (int i) {
		redis.clear();
		Set<String> keys = i!=0?jedis.keys("e"+i+"*"):jedis.keys("e"+"*");
		Iterator<String> it = keys.iterator();
		//Map<String, String> values = new HashMap<>();
		while (it.hasNext()) {
			String key= it.next();
			
			if (key.startsWith("v")||key.startsWith("e")){
				redis.put(key, jedis.hgetAll(key));
			}
		}
	}
	
	public static void neighbors(){
		//redis.clear();
		Set<String> keys = jedis.keys("doce*");
		//Map<String, Long> groupByCount = keys.stream().map(key->JReJSON.get(jedis, key, new Path(".age")).toString()).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
		//Map<String, String> groupByCount = keys.stream().map(key->jedis.hmget(key, "src", "tgt").toString()).collect(Collectors.groupingBy(it->it.toString().substring(0, it.indexOf(",")), Collectors.joining(",")));
		Map<String, String> groupByCount = keys.stream().map(key->JReJSON.get(jedis, key, new Path(".src"), new Path(".tgt")).toString()).collect(Collectors.groupingBy(it->it.toString().substring(0, it.indexOf(",")), Collectors.joining(",")));
		//
		/*groupByCount.entrySet().forEach(str->{
			System.out.println(str.getKey()+":"+str.getValue());
		});*/
		/*Iterator<String> it = keys.iterator();
		//Map<String, String> values = new HashMap<>();
		while (it.hasNext()) {
			String key= it.next();
			JReJSON.get(jedis, key, new Path(".age")).toString();
			//System.out.println(age.toString());
			//redis.put(key, jedis.hgetAll(key));
		}*/
	}
	
	public static void neighbors2(){
		//redis.clear();
		Set<String> keys = jedis.keys("doce*");
		//Map<String, Long> groupByCount = keys.stream().map(key->JReJSON.get(jedis, key, new Path(".age")).toString()).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
		//Map<String, String> groupByCount = keys.stream().map(key->jedis.hmget(key, "src", "tgt").toString().replace("[", "").replace("]","").replace(" ", "")).collect(Collectors.groupingBy(it->it.toString().substring(0, it.indexOf(",")), Collectors.joining(",")));
		Map<String, String> cleanedMap = new HashMap<>();
		
		
		Map<String, String> groupByCount = keys.stream().map(key->JReJSON.get(jedis, key, new Path(".src"), new Path(".tgt")).toString().replace(".src=", "").replace(".tgt=", "").replace("{", "").replace("}","").replace(" ", "")).collect(Collectors.groupingBy(it->it.toString().substring(0, it.indexOf(",")), Collectors.joining(",")));
		//
		groupByCount.entrySet().forEach(str->{
			cleanedMap.put(str.getKey(), str.getValue().replace(str.getKey()+",", ""));
		});
		Map<String, String> results = new HashMap<>();
		results.putAll(cleanedMap);
		cleanedMap.entrySet().forEach(str->{
			String tempStr = str.getValue();
			Set<String> split = new HashSet(Arrays.asList(str.getValue().split(",")));
			Set<String> result = new HashSet<>();
			result.addAll(split);
			split.forEach(sp->{
				if (cleanedMap.containsKey(sp)){
					result.addAll(Arrays.asList(cleanedMap.get(sp).split(",")));
				}
			});
			if (split.size()<result.size()){
				results.put(str.getKey(), result.stream().collect(Collectors.joining(",")));
			}
			//System.out.println(str.getKey()+":"+str.getValue());
		});
		//results.entrySet().forEach(str->{
		//	System.out.println(str.getKey()+":"+str.getValue());
		//});
		/*Iterator<String> it = keys.iterator();
		//Map<String, String> values = new HashMap<>();
		while (it.hasNext()) {
			String key= it.next();
			JReJSON.get(jedis, key, new Path(".age")).toString();
			//System.out.println(age.toString());
			//redis.put(key, jedis.hgetAll(key));
		}*/
	}
	
	public static void neighbors2Data(){
		redis.clear();
		
		Set<String> keys = jedis.keys("doce*");
		//Map<String, Long> groupByCount = keys.stream().map(key->JReJSON.get(jedis, key, new Path(".age")).toString()).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
		//Map<String, String> groupByCount = keys.stream().map(key->jedis.hmget(key, "src", "tgt").toString().replace("[", "").replace("]","").replace(" ", "")).collect(Collectors.groupingBy(it->it.toString().substring(0, it.indexOf(",")), Collectors.joining(",")));
		Map<String, String> cleanedMap = new HashMap<>();
		
		
		Map<String, String> groupByCount = keys.stream().map(key->JReJSON.get(jedis, key, new Path(".src"), new Path(".tgt")).toString().replace(".src=", "").replace(".tgt=", "").replace("{", "").replace("}","").replace(" ", "")).collect(Collectors.groupingBy(it->it.toString().substring(0, it.indexOf(",")), Collectors.joining(",")));
		//
		groupByCount.entrySet().forEach(str->{
			cleanedMap.put(str.getKey(), str.getValue().replace(str.getKey()+",", ""));
		});
		Set<String> mentionedVertexes = new HashSet<>();
		mentionedVertexes.addAll(cleanedMap.keySet());
		Map<String, String> results = new HashMap<>();
		results.putAll(cleanedMap);
		cleanedMap.entrySet().forEach(str->{
			String tempStr = str.getValue();
			Set<String> split = new HashSet(Arrays.asList(str.getValue().split(",")));
			Set<String> result = new HashSet<>();
			result.addAll(split);
			split.forEach(sp->{
				if (cleanedMap.containsKey(sp)){
					result.addAll(Arrays.asList(cleanedMap.get(sp).split(",")));
				}
			});
			if (split.size()<result.size()){
				results.put(str.getKey(), result.stream().collect(Collectors.joining(",")));
			}
			mentionedVertexes.addAll(result);
			//System.out.println(str.getKey()+":"+str.getValue());
		});
		
		
		/*Map<String, Map<String, String>> resultWithData = new HashMap<>();
		mentionedVertexes.stream().map(key->{
			Map<String,String> jedisAnswer = jedis.hgetAll(key);
			jedisAnswer.put("key", key);
			return jedisAnswer;}).forEach(item->{
			resultWithData.put(item.get("key"), item);
		});
		resultWithData.entrySet().forEach(item->{
			System.out.println(item.getKey());
			
		});*/
		
		Map<String,String> resultWithData = new HashMap<>();
		mentionedVertexes.forEach(key->{
			try{
			Object jedisAnswer = 
					JReJSON.get(jedis, "doc"+key, 
					new Path(".lastLogin"), 
					new Path(".completion"), 
					new Path(".registration"),
					new Path(".region"),
					new Path(".gender"), 
					new Path(".public"), 
					new Path(".age"), 
					new Path(".desc")
					);
			if (jedisAnswer!=null){
				resultWithData.put("doc"+key, jedisAnswer.toString());
			}} catch (Exception e){
				
			}
		});
/*		resultWithData.entrySet().forEach(item->{
			System.out.println(item.getKey());
			
		});*/
		
		//results.entrySet().forEach(str->{
		//	System.out.println(str.getKey()+":"+str.getValue());
		//});
		/*Iterator<String> it = keys.iterator();
		//Map<String, String> values = new HashMap<>();
		while (it.hasNext()) {
			String key= it.next();
			JReJSON.get(jedis, key, new Path(".age")).toString();
			//System.out.println(age.toString());
			//redis.put(key, jedis.hgetAll(key));
		}*/
	}
	
	
	public static void readAggregate(){
		//redis.clear();
		Set<String> keys = jedis.keys("v*");
		//Map<String, Long> groupByCount = keys.stream().map(key->JReJSON.get(jedis, key, new Path(".age")).toString()).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
		Map<String, Long> groupByCount = keys.stream().map(key->jedis.hget(key, "age").toString()).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
		
		groupByCount.entrySet().forEach(str->{
			System.out.println(str.getKey()+":"+str.getValue());
		});
		/*Iterator<String> it = keys.iterator();
		//Map<String, String> values = new HashMap<>();
		while (it.hasNext()) {
			String key= it.next();
			JReJSON.get(jedis, key, new Path(".age")).toString();
			//System.out.println(age.toString());
			//redis.put(key, jedis.hgetAll(key));
		}*/
	}
	
	public static void readV (int i) {
		redis.clear();
		Set<String> keys = i!=0?jedis.keys("v"+i+"*"):jedis.keys("v"+"*");
		Iterator<String> it = keys.iterator();
		//Map<String, String> values = new HashMap<>();
		while (it.hasNext()) {
			String key= it.next();
			
			if (key.startsWith("v")||key.startsWith("e")){
				redis.put(key, jedis.hgetAll(key));
			}
		}
	}
	
	//This converts all keys starting with v to vertexes and all starting with e to edges, in a graph with the name is passed as a parameter.
	//So far this works for a single type of node, passed as parameter
	//It assumes that the edge has a label (which defines the relationship type) and a src and tgt that have the nodeType values.
	public static void convertToGraph(String graphName, String nodeType, Boolean vertexes) {
		String script = "return {redis.call('GRAPH.QUERY', KEYS[1], ARGV[1])}";
		List<String> keys = new ArrayList<String>();
	    keys.add(graphName);
	    if (vertexes){
		redis.entrySet().stream().filter(e->e.getKey().startsWith("v")).forEach(v->{
			List<String> args = new ArrayList<String>();
			String query= "MATCH CREATE(";
			query+="graph"+v.getKey()+":"+nodeType+"";
			query+=" { id:\"graph"+v.getKey()+"\", ";
			query+=v.getValue().entrySet().stream().filter(entry->(!entry.getKey().equals(nodeType) && !entry.getKey().equals("desc"))).map(entry->entry.getKey()+":\""+entry.getValue().replaceAll("\"", "").replaceAll("\'",  "")+"\"").collect(Collectors.joining( ", " ) );
			query+="})";
			args.add(query);
//			System.out.println(query);
			List<ArrayList<String>> response = (List<ArrayList<String>>) jedis.eval(script, keys, args);
		   // response.forEach(resp->{
		   // 	System.out.println(resp.toString());
		    //});
			
		});
	    }
	    else{redis.entrySet().stream().filter(e->e.getKey().startsWith("e")).forEach(v->{
			List<String> args = new ArrayList<String>();
			String query= "'CREATE (";
			query+="graph"+v.getValue().get("src");
			//query+=")-["+v.getValue().get("label")+" {";
			query+=")-[friendsOf {";
			query+=v.getValue().entrySet().stream().filter(entry->(entry.getKey()!="label")&&entry.getKey()!="src" && entry.getKey()!="tgt").map(entry->entry.getKey()+":\""+entry.getValue()+"\"").collect(Collectors.joining( ", " ) );
			query+="}]->";
			query+="("+"graph"+v.getValue().get("tgt");
			query+=")";
			args.add(query);
			System.out.println(query);
			//List<ArrayList<String>> response = (List<ArrayList<String>>) jedis.eval(script, keys, args);
		});}
	}
	
	
	//This converts all keys starting with v to vertexes and all starting with e to edges, in a graph with the name is passed as a parameter.
	//So far this works for a single type of node, passed as parameter
	//It assumes that the edge has a label (which defines the relationship type) and a src and tgt that have the nodeType values.
	public static void convertToGraphStoredProcedure(String graphName, String nodeType) {
		String script = "return {redis.call('GRAPH.QUERY', KEYS[1], ARGV[1])}";
		List<String> keys = new ArrayList<String>();
	    keys.add(graphName);
		redis.entrySet().stream().filter(e->e.getKey().startsWith("v")).forEach(v->{
			List<String> args = new ArrayList<String>();
			String query= "'CREATE(";
			query+=v.getValue().get(nodeType)+":"+nodeType+"";
			query+="{ ";
			query+=v.getValue().entrySet().stream().filter(entry->entry.getKey()!=nodeType).map(entry->entry.getKey()+": "+entry.getValue()).collect(Collectors.joining( ", " ) );
			query+="})";
			args.add(query);
			List<ArrayList<String>> response = (List<ArrayList<String>>) jedis.eval(script, keys, args);
		    /*response.forEach(resp->{
		    	System.out.println(resp.toString());
		    });*/
			
		});
		redis.entrySet().stream().filter(e->e.getKey().startsWith("e")).forEach(v->{
			List<String> args = new ArrayList<String>();
			String query= "'CREATE (";
			query+=v.getValue().get("src");
			query+=")-["+v.getValue().get("label")+" {";
			query+=v.getValue().entrySet().stream().filter(entry->(entry.getKey()!="label")&&entry.getKey()!="src" && entry.getKey()!="tgt").map(entry->entry.getKey()+": "+entry.getValue()).collect(Collectors.joining( ", " ) );
			query+="}]->";
			query+="("+v.getValue().get("tgt");
			query+=")";
			args.add(query);
			List<ArrayList<String>> response = (List<ArrayList<String>>) jedis.eval(script, keys, args);
		    /*response.forEach(resp->{
		    	System.out.println(resp.toString());
		    });*/
		});
	}
	public static void convertToDocument() {
		//Gson gson = new GsonBuilder().create();
		redis.entrySet().forEach(entry->{
			//System.out.println(gson.toJson(entry.getValue()));
			JReJSON.set(jedis, "doc"+entry.getKey().toString(), entry.getValue());
		});
	}
	
	public static void convertToDocumentStoredProcedureV() {
		String script = "return {redis.call('GRAPH.QUERY', KEYS[1], ARGV[1])}";
		List<String> keys = new ArrayList<String>();
		List<String> args = new ArrayList<String>();
		List<ArrayList<String>> response = (List<ArrayList<String>>) jedis.eval(script, keys, args);
		
		//redis.entrySet().forEach(entry->{
			//JReJSON.set(jedis, entry.getKey().toString(), entry.getValue());
		//});
	}
	
	public static void convertToDocumentStoredProcedureE() {
		redis.entrySet().forEach(entry->{
			JReJSON.set(jedis, entry.getKey().toString(), entry.getValue());
		});
	}
	
	
	
	public static void cleanup() {
	    jedis.disconnect();
	  }
	
	public static void main(String[] args) {
		long startTime, endTime=0;
		int totalEdges= 0;
		Boolean vertexes = Boolean.FALSE;
		Boolean storedProcedure = Boolean.FALSE;
		Boolean document = Boolean.FALSE;
		init();
		startTime=System.nanoTime();
		//neighbors2Data();
		if (document){
			if (!storedProcedure){
				if (vertexes){
			
				for (int i=10; i<100; i++){
					readV(i);
					convertToDocument();				
				}
			}
			else{
			for (int i=10; i<100; i++){
				readE(i);
				convertToDocument();				
			}
			}}
			else{
			 if (vertexes){
				 convertToDocumentStoredProcedureV();
			 }	
			 else{
				 convertToDocumentStoredProcedureE();
			 }
			}
		}
		else if (vertexes){
			if (!storedProcedure){
				for (int i=10; i<100; i++){
					readV(i);
					convertToGraph("pokec", "profile", vertexes);
				}
			}
			else{
			}
		}
		else{
			if (!storedProcedure){
			//for (int i=100; i<110; i++){
				readE(0);
				//convertToDocument();
				convertToGraph("pokec", "profile", vertexes);
				totalEdges+=redis.size();
			//}
			}
			else{
			}
		}
		endTime=System.nanoTime()-startTime;
		System.out.println("Total duration = "+endTime);
		//System.out.println("Total edges"+totalEdges);
		cleanup();
	}

}
