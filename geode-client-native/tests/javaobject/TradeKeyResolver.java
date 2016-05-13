/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package javaobject;
import java.util.*;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.PartitionResolver;
import java.io.*;
import com.gemstone.gemfire.*;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.EntryOperation;
import javaobject.TradeKey;

  public class TradeKeyResolver implements PartitionResolver, Serializable ,Declarable
  {
    public String getName() {
      return "TradeKeyResolver";
    }
	
    public Serializable getRoutingObject(EntryOperation opDetails) {	 
	 TradeKey key = (TradeKey)opDetails.getKey();
	 int id = key.getId() + 5;
	 System.out.println("TradeKeyResolver::getRoutingObject() java side routing object = " + id);
     return id ;
    }
    public void close() {}
    public Properties getProperties() { return new Properties(); }
	public void init(Properties props)
	{
	}
  }
