/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * Restricted.java
 *
 * Created on October 4, 2005, 2:25 PM
 */

package com.gemstone.gemfire.cache.query.data;

/**
 *
 * @author  prafulla
 */
import java.io.Serializable;


public class Restricted implements Serializable{
    public int cusip;
    public String quoteType; 
    public String uniqueQuoteType;
    public double price;
    public int minQty;
    public int maxQty;
    public int incQty;    
    
    /** Creates a new instance of Restricted */
    public Restricted(int i) {
        cusip = 1000000000 - i;
        String [] arr1 = {"moving", "binding", "non binding", "not to exceed", "storage", "auto transport", "mortgage"};
        quoteType = arr1[i%7];
        uniqueQuoteType = "quoteType"+Integer.toString(i);
        price = (i/10)*8;
        minQty = i+100;
        maxQty = i+1000;
        if((i%12) == 0){
            incQty = maxQty - minQty;
        } else{
            incQty = ((maxQty - minQty)/12)*(i%12);
        }        
    }//end of constructor
    
    public int getCusip(){
    	return cusip;
    }
    
    public String getQuoteType(){
    	return quoteType;
    }
    
    public String getUniqueQuoteType(){
    	return quoteType;
    }
    
    public int getMinQty(){
    	return minQty;
    }
    
    public int getMaxQty(){
    	return maxQty;
    }
    
    public double getPrice(){
    	return price;
    }
}//end of Restricted
