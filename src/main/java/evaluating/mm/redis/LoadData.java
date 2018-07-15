package evaluating.mm.redis;


import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.redislabs.modules.rejson.Path;
import com.sun.org.apache.xerces.internal.impl.xpath.regex.ParseException;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

public class LoadData {
	
private static Jedis jedis;
	
	private static Map<String, String> redis;
	public static void init () {
		redis = new HashMap<>();
		jedis = new Jedis("127.0.0.1", Protocol.DEFAULT_PORT, 500);
		jedis.connect();
	}

	public static void main(String[] args) throws IOException, java.text.ParseException {

		/*Map<String, ArrayList<String>> nodes = new HashMap<String, ArrayList<String>>();
		
		BufferedReader in = new BufferedReader(new FileReader("Data1.txt"));
		
		ArrayList<String> node_values = new ArrayList<String>();
		
		String line = "";
        while ((line = in.readLine()) != null) {
            String parts[] = line.split("\t");
            node_values.add(line);*/
        init();
		try(Stream<java.nio.file.Path> paths = Files.walk(Paths.get("/home/campero/Desktop/current/pokec")).sorted()) {
			List<java.nio.file.Path> pathsFound = paths.collect(Collectors.toList()); 
			for (java.nio.file.Path path: pathsFound){
				String pathAsString = path.toString();
            
				if (pathAsString.endsWith("profiles.txt")){
				
					BufferedReader in = new BufferedReader(new FileReader(path.toFile()));
					String line, desc, nodeId;
					Integer count=0; 
					while((line = in.readLine()) != null)
					{	
						count++;
						if (count<0){
							//do nothing
						}
						else{
								String[] split = line.split("\t", -1);
								if (split.length>9){
									if (!redis.isEmpty()){
										redis.clear();
									}
									nodeId= split[0];
									redis.put("public", Boolean.TRUE.toString());
									if (split[2]!=null && !split[2].contains("null")){
										Integer completion= Integer.parseInt(split[2]);
										redis.put("completion", completion.toString());
									}
									else{
										redis.put("completion", "0");
									}
									if (split[3]!=null && !split[3].contains("null")){
										Boolean gender= split[3].contains("1");
										redis.put("gender", gender.toString());
									}
									else{
										redis.put("gender", Boolean.FALSE.toString());
									}
									redis.put("region", split[4]);
									redis.put("lastLogin", split[5]);
									redis.put("registration", split[6]);
									if (split[7]!=null && !split[7].contains("null")){
										Integer age=Integer.parseInt(split[7]);
										redis.put("age", age.toString());
									}
									else{
										redis.put("age", "0");
									}
									desc="";
									for (int i=8; i<split.length; i++){
										desc+=split[i]+" ";
									}
									redis.put("desc", desc);
									jedis.hmset("v"+nodeId, redis);
						
					}
            
        }
						}
					System.out.println("W00T, we loaded the vertexes");
		}
		else if (pathAsString.endsWith("relationships.txt")){
						
						BufferedReader in = new BufferedReader(new FileReader(path.toFile()));
						String line, desc, nodeId;
						Integer count=0; 
						while((line = in.readLine()) != null)
						{	
							count++;
							if (count> 100000){
								//do nothing
							}
							else{
									String[] split = line.split("\t", -1);
									if (split.length>1){
										if (!redis.isEmpty()){
											redis.clear();
										}
										nodeId= count.toString();
										if (split[0]!=null && !split[0].contains("null")){
											redis.put("src", "v"+split[0]);
										}
										if (split[1]!=null && !split[1].contains("null")){
											redis.put("tgt", "v"+split[1]);
										}
										redis.put("label", "friendsOf");
										if (redis.containsKey("tgt")&&redis.containsKey("src")){
											jedis.hmset("e"+nodeId, redis);
										}
										else{
											System.out.println("Insufficient data");
										}
										
						}
	            
	        }
							}
						System.out.println("W00T, we loaded the edges");
						}	
				
			}
		}
		
		
	}
}

            
//.put(parts[0], parts[1]);
		
		
  //      valSetOne.add("");
    //    valSetOne.add("");
        
      

	

