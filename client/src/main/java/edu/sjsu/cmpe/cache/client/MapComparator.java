package edu.sjsu.cmpe.cache.client;

import java.util.Comparator;
import java.util.HashMap;

public class MapComparator implements Comparator<String>{

	HashMap<String, Integer> maptosort;
	
	public MapComparator(HashMap<String, Integer> maptosort) {
		this.maptosort = maptosort;
	}
	
	
	@Override
	public int compare(String k1, String k2) {
		// TODO Auto-generated method stub
		if(maptosort.get(k1) >= maptosort.get(k2)){
			return -1;
		}else{
			return 1;
		}
	}

}
