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

  public class ModPartitionResolver implements PartitionResolver, Serializable ,Declarable
  {
    public String getName() {
      return getClass().getName();
    }

    public Serializable getRoutingObject(EntryOperation opDetails) {
    	 ModRoutingObject routingObject = new ModRoutingObject(opDetails.getKey());
    	    return routingObject;
    }
    public void close() {}
    public Properties getProperties() { return new Properties(); }
        public void init(Properties props)
        {
        }
  }
