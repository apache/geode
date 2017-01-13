/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package javaobject;
import java.util.*;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.PartitionResolver;
import java.io.*;
import org.apache.geode.*;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.EntryOperation;
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
