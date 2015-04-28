/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * District.java
 *
 * Created on September 30, 2005, 6:16 PM
 */

package com.gemstone.gemfire.cache.query.data;

import java.util.Set;
import java.io.*;

/**
 *
 * @author prafulla
 */
public class District implements Serializable{
   public String name; 
   public Set cities; 
   public Set villages;
    /** Creates a new instance of District */
    public District(String name, Set cities, Set villages) {
        this.name = name;
        this.cities = cities;
        this.villages = villages;
    }//end of contructor 1
  
     public District(int i, Set cities, Set villages) {
        String arr1 [] = {"MUMBAIDIST", "PUNEDIST","GANDHINAGARDIST","CHANDIGARHDIST", "KOLKATADIST"};
        /*this is for the test to have 20% of the objects belonging to one districtr*/
        this.name = arr1[i%5];
        this.cities = cities;
        this.villages = villages;
    }//end of contructor 2
     
     ///////////////////////////////
     
    public String getName(){
        return name;
    }
    
    public Set getCities(){
        return cities;
    }
    
    public Set getVillages(){
        return villages;
    }
}// end of class
