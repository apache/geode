/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * Village.java
 *
 * Created on September 30, 2005, 6:23 PM
 */

package com.gemstone.gemfire.cache.query.data;

/**
 *
 * @author prafulla
 */
import java.io.*;

public class Village implements Serializable{
    public String name;
     public int zip;
    /** Creates a new instance of Village */
    public Village(String name, int zip) {
        this.name = name;
        this.zip = zip;
    }//end of constructor 1
    
    public Village(int i) {
       String arr1 [] = {"MAHARASHTRA_VILLAGE1","PUNJAB_VILLAGE1","KERALA_VILLAGE1", "GUJARAT_VILLAGE1"};
        this.name = arr1[i%4];
        this.zip = 425125 + i;
    }//end of constructor 2
    
    ///////////////////////////////
    
      public String getName(){
        return name;
    }
    
    public int getZip(){
        return zip;
    }
}//end of class
