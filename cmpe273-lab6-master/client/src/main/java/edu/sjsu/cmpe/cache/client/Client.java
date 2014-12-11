package edu.sjsu.cmpe.cache.client;

import java.util.ArrayList;

public class Client {

    public static void main(String[] args) throws Exception {
        
    	
    	ArrayList<CacheServiceInterface> servers = new ArrayList<CacheServiceInterface>();
    	//list of servers
	    CacheServiceInterface cache1 = new DistributedCacheService("http://localhost:3000");
	    CacheServiceInterface cache2 = new DistributedCacheService("http://localhost:3001");
	    CacheServiceInterface cache3 = new DistributedCacheService("http://localhost:3002");
	    servers.add(cache1);
	    servers.add(cache2);
	    servers.add(cache3);
    	
	    //add key to all servers
	    WriteDistributedCache writeDistributedCache = new WriteDistributedCache(servers);
	    writeDistributedCache.writeKeyToDistributedCache(1, "a");
	    System.out.println("write 1 end");
	    
	    System.out.println("now thread is sleeping...Down server 3000");
	    Thread.sleep(20000);
	    System.out.println("now thread is up");
	    //add key to all servers
	    servers.remove(0);
	    System.out.println(servers.size());
	    WriteDistributedCache writeDistributedCache1 = new WriteDistributedCache(servers);
	    writeDistributedCache1.writeKeyToDistributedCache(1, "b");
	    System.out.println("write 2 end");
	    
	    System.out.println("now thread is sleeping...up server 3000");
	    Thread.sleep(20000);
	    System.out.println("now thread is up again");
	    servers.add(new DistributedCacheService("http://localhost:3000"));
	    System.out.println(servers.size());
	    //read key from all servers
	    ReadDistributedCache readDistributedCache = new ReadDistributedCache(servers);
	    readDistributedCache.readValueToDistributedCache(1);
	    System.out.println("read end");
	    /*System.out.println(cache1.get(1));
	    System.out.println(cache2.get(1));
	    System.out.println(cache3.get(1));*/
	    
    }

}
