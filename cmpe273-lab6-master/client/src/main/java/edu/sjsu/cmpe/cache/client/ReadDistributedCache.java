package edu.sjsu.cmpe.cache.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Future;

import org.apache.http.entity.mime.Header;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.async.Callback;
import com.mashape.unirest.http.exceptions.UnirestException;

public class ReadDistributedCache {



	static int countOfCacheServer;
	public ArrayList<CacheServiceInterface> servers;
	public static HashMap<String, Integer> readValueStorage = new HashMap<String, Integer>();
	public HashMap<String, String> serverListValueStorage = new HashMap<String, String>();
	
	public int succcessCounter = 0;
	public int failureCounter = 0;
	public int waitingResponseCounter = 0;
	
	public ArrayList<CacheServiceInterface> successServers = new ArrayList<CacheServiceInterface>();
	public int keyToAdd;
	public int transactionSucess = -1;
	

	ReadDistributedCache(ArrayList<CacheServiceInterface> servers) {
		this.servers = servers;
	}

	public void callBackReadCheck() {

		if (waitingResponseCounter == 0 && succcessCounter == 3) {
			// check for repair
			String repairValue = getRepairValue();
			//sending to reapir put and check whether to repair is need or not there itself
			repairCacheFromDistributedCache(repairValue);
			
		}else{
			//do nothing
			
		}

	}
	
	public void readValueToDistributedCache(int key) {

		boolean isWriteSuccess = false;
		keyToAdd = key;

		// reset counter
		this.succcessCounter = 0;
		this.failureCounter = 0;
		this.waitingResponseCounter = servers.size();

		// aynchronous call on clients
		for (final CacheServiceInterface server : servers) {
			Future<HttpResponse<JsonNode>> response = null;
			String tempServer = server.toString();
			try {
				response = Unirest
						.get(server.toString() + "/cache/{key}")
						.header("accept", "application/json")
						.routeParam("key", Long.toString(key))
						.asJsonAsync(new Callback<JsonNode>() {

							@Override
							public void failed(UnirestException e) {
								System.out.println("failed");
								waitingResponseCounter--;
								failureCounter++;
								callBackReadCheck();

							}

							@Override
							public void completed(
									HttpResponse<JsonNode> response) {
								System.out.println("Yes 200");
								waitingResponseCounter--;
								succcessCounter++;
								System.out.println("server -->"+server);
								String value = "";
								if(response.getBody() != null){
									value = response.getBody().getObject().getString("value");
								}
						        System.out.println("value -->"+value);
								serverListValueStorage.put(server.toString(), value);
								Integer getExistingCounter = readValueStorage.get(value);
								if(getExistingCounter == null){
									//add
									readValueStorage.put(value, 1);
								}else{
									//update
									readValueStorage.put(value, getExistingCounter+1);
								}
								callBackReadCheck();
								System.out.println(".......");
							}

							@Override
							public void cancelled() {
								System.out.println("cancelled");
								waitingResponseCounter--;
								failureCounter++;
								callBackReadCheck();

							}
						});
			} catch (Exception e) {
				e.printStackTrace();
			}

			

		}
	}
	
	//repair cache from all servers
	public void repairCacheFromDistributedCache(String repairValue){

		boolean isDeleteSuccess = false;

		// reset counter
		this.succcessCounter = 0;
		this.failureCounter = 0;
		this.waitingResponseCounter = successServers.size();

		ArrayList<CacheServiceInterface> updateNeedServer = new ArrayList<CacheServiceInterface>();
		
		// aynchronous call on clients
		for (Entry<String, String> server : serverListValueStorage.entrySet()) {

			if(server.getValue() != null && !server.getValue().equals(repairValue)){
				//need of repair
				System.out.println(server.getKey() + " --> " + server.getValue() + " update to " + repairValue);
				updateNeedServer.add(new DistributedCacheService(server.getKey().toString()));
			}
			
		}
		//check update list 
		if(updateNeedServer.size() >  0){
			System.out.println("update required "+updateNeedServer.size()+" for "+keyToAdd+" --> "+repairValue);
			WriteDistributedCache writeDistributedCache = new WriteDistributedCache(updateNeedServer);
			 writeDistributedCache.writeKeyToDistributedCache(keyToAdd, repairValue);
		}
		

	}


	private String getRepairValue(){
		
		MapComparator mapComparator = new MapComparator(readValueStorage);
		SortedMap<String, Integer> sortedMap = new TreeMap<String, Integer>(mapComparator);
		sortedMap.putAll(readValueStorage);
		return sortedMap.firstKey();
		
	}
	
	
}
