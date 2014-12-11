package edu.sjsu.cmpe.cache.client;

import java.awt.List;
import java.util.ArrayList;
import java.util.Map;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.async.Callback;
import com.mashape.unirest.http.exceptions.UnirestException;

public class WriteDistributedCache {

	public ArrayList<CacheServiceInterface> servers;
	public int succcessCounter = 0;
	public int failureCounter = 0;
	public int waitingResponseCounter = 0;
	
	public ArrayList<CacheServiceInterface> successServers = new ArrayList<CacheServiceInterface>();
	public int keyToAdd;
	public int transactionSucess = -1;

	WriteDistributedCache(ArrayList<CacheServiceInterface> servers) {
		this.servers = servers;
	}

	public void callBackWriteCheck() {

		if (waitingResponseCounter == 0 && failureCounter >= 2) {
			// send all to undo write
			
		}

	}

	public void callBackDeleteCheck() {

		if (waitingResponseCounter == 0 && failureCounter == 0) {
			//transaction sucess
			transactionSucess++;
		}else{
			//delete again
			deleteCacheFromDistributedCache(keyToAdd);
		}

	}

	
	public void writeKeyToDistributedCache(int key, String value) {

		boolean isWriteSuccess = false;
		keyToAdd = key;

		// reset counter
		this.succcessCounter = 0;
		this.failureCounter = 0;
		this.waitingResponseCounter = servers.size();

		// aynchronous call on clients
		for (CacheServiceInterface server : servers) {
			System.out.println("#"+server+"#"+key+"#"+value);
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
								System.out.println("failed");
								waitingResponseCounter--;
								failureCounter++;
								callBackWriteCheck();

							}

							@Override
							public void completed(
									HttpResponse<JsonNode> response) {
								if (response.getCode() != 200) {
									System.out.println("No 200");
									waitingResponseCounter--;
									failureCounter++;
								} else {
									System.out.println("Yes 200");
									waitingResponseCounter--;
									succcessCounter++;
								}
								callBackWriteCheck();

							}

							@Override
							public void cancelled() {
								System.out.println("cancelled");
								waitingResponseCounter--;
								failureCounter++;
								callBackWriteCheck();

							}
						}).get();
			} catch (Exception e) {
				e.printStackTrace();
			}

			if (response.getCode() != 200) {
				System.out.println("Failed to add to the cache.");
			}

		}
	}
	
	//delete cache from all servers
	public void deleteCacheFromDistributedCache(int key){

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
								System.out.println("delete failed");
								waitingResponseCounter--;
								failureCounter++;
								callBackDeleteCheck();

							}

							@Override
							public void completed(
									HttpResponse<JsonNode> response) {
								if (response.getCode() != 200) {
									System.out.println("delete NO 200");
									waitingResponseCounter--;
									failureCounter++;
								} else {
									System.out.println("delete Yes 200");
									waitingResponseCounter--;
									succcessCounter++;
								}
								callBackWriteCheck();

							}

							@Override
							public void cancelled() {
								System.out.println("Cancelled");
								waitingResponseCounter--;
								failureCounter++;
								callBackWriteCheck();

							}
						}).get();
			} catch (Exception e) {
				System.err.println(e);
			}

			if (response.getCode() != 200) {
				System.out.println("Failed to add to the cache.");
			}

		}

	}

}
