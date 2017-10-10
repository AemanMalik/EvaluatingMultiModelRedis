package evaluating.mm.redis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import com.redislabs.modules.rejson.JReJSON;

import evaluating.mm.redis.utils.Commands;

public class RedisClient {
	
	private static Jedis jedis;
	
	private static Map<String, Map<String,String>> redis;
	
	public static void init () {
		redis = new HashMap<>();
		jedis = new Jedis("127.0.0.1", Protocol.DEFAULT_PORT);
		jedis.connect();
	}
	
	public static void read () {
		Set<String> keys = jedis.keys("*");
		Iterator<String> it = keys.iterator();
		while (it.hasNext()) {
			String key= it.next();
			if (key.startsWith("v")||key.startsWith("e")){
				Map<String, String> values = jedis.hgetAll(key);
				redis.put(key, values);
			}
		}
		
	}
	
	//This converts all keys starting with v to vertexes and all starting with e to edges, in a graph with the name is passed as a parameter.
	//So far this works for a single type of node, passed as parameter
	//It assumes that the edge has a label (which defines the relationship type) and a src and tgt that have the nodeType values.
	public static void convertToGraph(String graphName, String nodeType) {
		String script = "return {redis.call('GRAPH.QUERY', KEYS[1], ARGV[1])}";
		List<String> keys = new ArrayList<String>();
	    keys.add(graphName);
		redis.entrySet().stream().filter(e->e.getKey().startsWith("v")).forEach(v->{
			List<String> args = new ArrayList<String>();
			String query= "'CREATE (";
			query+=v.getValue().get(nodeType)+":"+nodeType+" ";
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
	
	
	//This converts all keys starting with v to vertexes and all starting with e to edges, in a graph with the name is passed as a parameter.
	//So far this works for a single type of node, passed as parameter
	//It assumes that the edge has a label (which defines the relationship type) and a src and tgt that have the nodeType values.
	public static void convertToGraphStoredProcedure(String graphName, String nodeType) {
		String script = "return {redis.call('GRAPH.QUERY', KEYS[1], ARGV[1])}";
		List<String> keys = new ArrayList<String>();
	    keys.add(graphName);
		redis.entrySet().stream().filter(e->e.getKey().startsWith("v")).forEach(v->{
			List<String> args = new ArrayList<String>();
			String query= "'CREATE (";
			query+=v.getValue().get(nodeType)+":"+nodeType+" ";
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
		redis.entrySet().forEach(entry->{
			JReJSON.set(jedis, entry.getKey().toString(), entry.getValue());
		});
	}
	
	public static void convertToDocumentStoredProcedure() {
		redis.entrySet().forEach(entry->{
			JReJSON.set(jedis, entry.getKey().toString(), entry.getValue());
		});
	}
	
	
	
	public static void cleanup() {
	    jedis.disconnect();
	  }
	
	public static void main(String[] args) {
		long startTime, endTime=0;
		init();
		startTime=System.nanoTime();
		read();
		//convertToDocument();
		convertToGraph("pokec", "profile");
		endTime=System.nanoTime()-startTime;
		System.out.println("Total duration = "+endTime);
		cleanup();
	}

}
