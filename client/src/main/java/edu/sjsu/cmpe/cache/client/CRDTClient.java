package edu.sjsu.cmpe.cache.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.Future;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.async.Callback;
import com.mashape.unirest.http.exceptions.UnirestException;

public class CRDTClient {

	//read
	static int countOfCacheServer;
	public ArrayList<CacheServiceInterface> servers;
	public String previousValue;
	public ArrayList<CacheServiceInterface> failedWriteServers;
	public static HashMap<String, Integer> readValueStorage = new HashMap<String, Integer>();
	public HashMap<String, String> serverListValueStorage = new HashMap<String, String>();
	
	public int succcessCounter = 0;
	public int failureCounter = 0;
	public int waitingResponseCounter = 0;
	
	public ArrayList<CacheServiceInterface> successServers = new ArrayList<CacheServiceInterface>();
	public int keyToAdd;
	public int transactionSucess = -1;
	

	CRDTClient(ArrayList<CacheServiceInterface> servers) {
		this.servers = servers;
	}

	
	//write
	public void callBackWriteCheck() {

		if (waitingResponseCounter == 0 && failureCounter >= 2) {
			// send all to undo write
			try {
				deleteCacheFromDistributedCache(keyToAdd);
				writeFailedRollback();
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	public void callBackDeleteCheck() throws IOException {

		if (waitingResponseCounter == 0 && failureCounter == 0) {
			//transaction sucess
			transactionSucess++;
		}else{
			//delete again
			deleteCacheFromDistributedCache(keyToAdd);
		}

	}

	
	public void writeKeyToDistributedCache(int key, String value) throws IOException {

		boolean isWriteSuccess = false;
		keyToAdd = key;

		// reset counter
		this.succcessCounter = 0;
		this.failureCounter = 0;
		this.waitingResponseCounter = servers.size();
		
		//make consistent state
		//readValueToDistributedCache(keyToAdd);
		//save previous value
		savePartialState();
		
		// reset counter
				this.succcessCounter = 0;
				this.failureCounter = 0;
				this.waitingResponseCounter = servers.size();
				this.successServers = new ArrayList<CacheServiceInterface>();
		
		failedWriteServers = new ArrayList<CacheServiceInterface>();

		// aynchronous call on clients
		for (final CacheServiceInterface server : servers) {
			HttpResponse<JsonNode> response = null;
			try {
				response = Unirest
						.put(server.toString() + "/cache/{key}/{value}")
						.header("accept", "application/json")
						.routeParam("key", Long.toString(key))
						.routeParam("value", value)
						.asJsonAsync(new Callback<JsonNode>() {

							@Override
							public void failed(UnirestException e) {
								waitingResponseCounter--;
								failureCounter++;
								callBackWriteCheck();
								failedWriteServers.add(server);
							}

							@Override
							public void completed(
									HttpResponse<JsonNode> response) {
								if (response.getCode() != 200) {
									waitingResponseCounter--;
									failureCounter++;
								} else {
									waitingResponseCounter--;
									succcessCounter++;
									successServers.add(server);
								}
								callBackWriteCheck();

							}

							@Override
							public void cancelled() {
								waitingResponseCounter--;
								failureCounter++;
								callBackWriteCheck();

							}
						}).get();
			} catch (Exception e) {
				//e.printStackTrace();
			}

			if (response == null || response.getCode() != 200) {
			}

		}
		
	}
	
	//delete cache from all servers
	public void deleteCacheFromDistributedCache(int key) throws IOException{
		
		boolean isDeleteSuccess = false;

		// reset counter
		this.succcessCounter = 0;
		this.failureCounter = 0;
		this.waitingResponseCounter = successServers.size();

		// aynchronous call on clients
		for (CacheServiceInterface server : successServers) {

			HttpResponse<JsonNode> response = null;
			try {
				response = Unirest
						.delete(server.toString() + "/cache/{key}")
						.header("accept", "application/json")
						.routeParam("key", Long.toString(key))
						.asJsonAsync(new Callback<JsonNode>() {

							@Override
							public void failed(UnirestException e) {
								waitingResponseCounter--;
								failureCounter++;
								try {
									callBackDeleteCheck();
								} catch (IOException e1) {
									// TODO Auto-generated catch block
									e1.printStackTrace();
								}

							}

							@Override
							public void completed(
									HttpResponse<JsonNode> response) {
								if (response.getCode() != 200) {
									waitingResponseCounter--;
									failureCounter++;
								} else {
									waitingResponseCounter--;
									succcessCounter++;
								}
								callBackWriteCheck();

							}

							@Override
							public void cancelled() {
								waitingResponseCounter--;
								failureCounter++;
								callBackWriteCheck();

							}
						}).get();
			} catch (Exception e) {
				System.err.println(e);
			}

			if (response.getCode() != 200) {
			}

		}
		
	}
	
	
	//read
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
	
	public void readValueToDistributedCache(int key) throws IOException {

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
								waitingResponseCounter--;
								failureCounter++;
								callBackReadCheck();

							}

							@Override
							public void completed(
									HttpResponse<JsonNode> response) {
								waitingResponseCounter--;
								succcessCounter++;
								String value = "";
								if(response.getBody() != null){
									value = response.getBody().getObject().getString("value");
								}
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
							}

							@Override
							public void cancelled() {
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
				updateNeedServer.add(new DistributedCacheService(server.getKey().toString()));
			}
			
		}
		//check update list 
		if(updateNeedServer.size() >  0){
			
			for(CacheServiceInterface server : updateNeedServer){
				System.out.println("Repairing on "+ server.toString());
				try {
					HttpResponse<JsonNode> response = Unirest.put(server.toString() + "/cache/{key}/{value}")
							.header("accept", "application/json")
							.routeParam("key", Long.toString(keyToAdd))
							.routeParam("value", repairValue)
							.asJson();
				} catch (UnirestException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
		}else{
			
		}
		try {
			Unirest.shutdown();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	private String getRepairValue(){
		
		MapComparator mapComparator = new MapComparator(readValueStorage);
		SortedMap<String, Integer> sortedMap = new TreeMap<String, Integer>(mapComparator);
		sortedMap.putAll(readValueStorage);
		return sortedMap.firstKey();
		
	}
	
	private void savePartialState(){
		for(CacheServiceInterface server:servers){
			try {
				HttpResponse<JsonNode> response = Unirest.get(server + "/cache/{key}")
	                    .header("accept", "application/json")
	                    .routeParam("key", Long.toString(keyToAdd)).asJson();
				previousValue = response.getBody().getObject().getString("value");
				if(previousValue == null)continue;
				else break;
	        } catch (Exception e) {
	        	continue;
	        }
		}
	}
	
	
	private void writeFailedRollback(){
		//delete
		//put to partial state
		for(CacheServiceInterface successServers : this.successServers){
			String prevValue = previousValue;
			//put it
			try {
				HttpResponse<JsonNode> response = Unirest.put(successServers.toString() + "/cache/{key}/{value}")
						.header("accept", "application/json")
						.routeParam("key", Long.toString(keyToAdd))
						.routeParam("value", prevValue)
						.asJson();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
			}
		}
		
	}
	
}
