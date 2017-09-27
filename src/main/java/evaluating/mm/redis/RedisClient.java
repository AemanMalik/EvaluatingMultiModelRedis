package evaluating.mm.redis;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import com.redislabs.modules.rejson.JReJSON;

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
			Map<String, String> values = jedis.hgetAll(key);
			redis.put(key, values);
		}
		
	}
	
	public static void convertToGraph() {
		
	}
	
	public static void convertToDocument() {
		redis.entrySet().forEach(entry->{
			JReJSON.set(jedis,entry.getKey(), entry.getValue());
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
		convertToDocument();
		endTime=startTime-System.nanoTime();
		System.out.println("Total duration = "+endTime);
		cleanup();
	}

}
