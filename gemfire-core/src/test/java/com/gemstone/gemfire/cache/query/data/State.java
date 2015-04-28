/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * State.java
 *
 * Created on September 30, 2005, 6:10 PM
 */

package com.gemstone.gemfire.cache.query.data;

import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;
import java.io.*;

/**
 *
 * @author prafulla
 */
public class State implements Serializable{
    public String name;
    public String zone;
    public Set districts;
    /** Creates a new instance of State */
    public State(String name, String zone, Set districts) {
        this.name = name;
        this.zone = zone;
        this.districts = districts;
    }//end of contructor 1
    
    public State(int i, Set districts) {
        String arr1 [] = {"MAHARASHTRA", "GUJARAT","PUNJAB","KERALA","AASAM"};
        String arr2 [] = {"WEST", "WEST","NORTH","SOUTH","EAST"}; 
        /*this is for the test to have 33.33% of the objects belonging to one state*/
        this.name = arr1[i%3];
        this.zone = arr2[i%3];
        this.districts = districts;
    }//end of contructor 2
    
    //////////////////////////////
  
    public String getName(){
        return name;
    }
    
     public String getZone(){
        return zone;
    }
      public Set getDistricts(){
        return districts;
    }
      
      
    public Set getDistrictsWithSameName(District dist){
    	Set districtsWithSameName = new HashSet();    	
    	Iterator itr2 = districts.iterator();
    	District dist1;
    	while(itr2.hasNext()){    		 
    		 dist1 = (District)itr2.next();
    		 if(dist1.getName().equalsIgnoreCase(dist.getName())){
    			 districtsWithSameName.add(dist1);
    		 }    		 
    	}    	
    	return districtsWithSameName;
    }//end of   getDistrictsWithSameName
      
}//end of class
