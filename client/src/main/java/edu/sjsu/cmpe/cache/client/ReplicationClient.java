package edu.sjsu.cmpe.cache.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class ReplicationClient {
	
	public ConcurrentHashMap<String, String> putStatus = new ConcurrentHashMap<String, String>();
	public ConcurrentHashMap<String, String> getStatus = new ConcurrentHashMap<String, String>();
	private ArrayList<DistributedCacheService> cacheServers = new ArrayList<DistributedCacheService>();
	
	public void addServer(String serverURL) {
        cacheServers.add(new DistributedCacheService(serverURL,this));
	}
	
	
	public void put(long key, String value) {
		for(DistributedCacheService service: cacheServers) {
			service.put(key, value);
		}
		
		while(true) {
        	if(putStatus.size() < 3) {
        		try {
        			System.out.println("PUT Requests Under Processing");
					Thread.sleep(400);
				} catch (InterruptedException e) {
				  e.printStackTrace();
				}
        	} else{
        		int failed = 0;
        		int success = 0;
        		for(DistributedCacheService service: cacheServers) {
        			System.out.println("Put Status of : "+service.getCacheServerURL()+": "+putStatus.get(service.getCacheServerURL()));
        			if(putStatus.get(service.getCacheServerURL()).equalsIgnoreCase("fail"))
            			failed++;
            		else
            			success++;
        		}
        		
        		if(failed > 1) {
        			System.out.println("Reverting put operations on all servers");
        			for(DistributedCacheService service: cacheServers) {
        				service.delete(key);
        			}
        		} else {
        			System.out.println("Successfully updated servers");
        		}
        		putStatus.clear();
        		break;
        	}
        }
	}
	
	public String get(long key){
		for(DistributedCacheService service: cacheServers) {
			service.get(key);
		}
		
		while(true) {
        	if(getStatus.size() < 3) {
        		try {
        			System.out.println("GET Requests Under Processing");
					Thread.sleep(400);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
        	} else{
        		HashMap<String, List<String>> mapStore = new HashMap<String, List<String>>();
        		for(DistributedCacheService service: cacheServers) {
        			if(getStatus.get(service.getCacheServerURL()).equalsIgnoreCase("fail"))
            			System.out.println("Unable to retrieve get value from server: "+service.getCacheServerURL());
            		else {
            			if(mapStore.containsKey(getStatus.get(service.getCacheServerURL()))) {
                            mapStore.get(getStatus.get(service.getCacheServerURL())).add(service.getCacheServerURL());
            			} else {
            				List<String> tempList = new ArrayList<String>();
            				tempList.add(service.getCacheServerURL());
                            mapStore.put(getStatus.get(service.getCacheServerURL()),tempList);
            			}
            		}
        		}
        		
        		if(mapStore.size() != 1) {
        			System.out.println("Inconsistent Values Prevailing on Servers");
        			Iterator<Entry<String, List<String>>> iterator = mapStore.entrySet().iterator();
        			int majority = 0;
        			String finVal = null;
        			ArrayList <String> updateServer = new ArrayList<String>();
        		    while (iterator.hasNext()) {
        		        Map.Entry<String, List<String>> map = (Map.Entry<String, List<String>>)iterator.next();
        		        if(map.getValue().size() > majority) {
        		        	majority = map.getValue().size();
        		        	finVal = map.getKey();
        		        } else {
        		        	for (String str: map.getValue()){
        		        		updateServer.add(str);
        		        	}
        		        }
        		    }
        		    
        			System.out.println("Repairing Cache Servers for Consistency");
        			for(String str: updateServer){
        				for(DistributedCacheService service: cacheServers) {
            				if(service.getCacheServerURL().equalsIgnoreCase(str)){
            					System.out.println("Repair on server: "+service.getCacheServerURL()+" as: "+ finVal);
            					service.put(key, finVal);
            				}
            			}
        			}
                    getStatus.clear();
        			return finVal;
        		} else {
        			System.out.println("Get Operation on cacheServers Successful");
                    getStatus.clear();
        			return mapStore.keySet().toArray()[0].toString();
        		}
        	}
        }
		
	}

}
