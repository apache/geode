/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * City.java
 *
 * Created on September 30, 2005, 6:20 PM
 */

package com.gemstone.gemfire.cache.query.data;

/**
 *
 * @author prafulla
 */
import java.io.*;

public class City implements Serializable{
    public String name;
    public int zip;
    /** Creates a new instance of City */
    public City(String name, int zip) {
        this.name = name;
        this.zip = zip;
    }//end of constructor 1
    
    public City(int i) {
        String arr1 [] = {"MUMBAI", "PUNE","GANDHINAGAR","CHANDIGARH"};
        /*this is for the test to have 50% of the objects belonging to one city*/
        this.name = arr1[i%2];
        this.zip = 425125 + i;
    }//end of constructor 2
    
    ////////////////////////////
    
    public String getName(){
        return name;
    }
    
    public int getZip(){
        return zip;
    }
}//end of class
