/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package javaobject;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.cache.execute.InternalFunctionInvocationTargetException;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.execute.PartitionedRegionFunctionResultSender;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.cache.GemFireCacheImpl;

public class RegionOperationsHAFunctionPrSHOP extends FunctionAdapter implements
    Declarable {

  public void execute(FunctionContext context) {
  
	RegionFunctionContext regionContext = (RegionFunctionContext)context;
    PartitionedRegion pr = (PartitionedRegion)regionContext.getDataSet();
	PartitionedRegionFunctionResultSender rs = (PartitionedRegionFunctionResultSender)regionContext.getResultSender() ;
	
    ArrayList vals = new ArrayList();
    if (context.getArguments() instanceof Boolean) {
	  
	  if(rs.isLocallyExecuted()){
	   context.getResultSender().lastResult((Boolean)context.getArguments());
	  } else {
	   context.getResultSender().sendResult((Boolean)context.getArguments());
	   pr.getCache().getDistributedSystem().disconnect();
      }      
    }
    else if (context.getArguments() instanceof String) {
      String key = (String)context.getArguments();
	  if(rs.isLocallyExecuted()){
	   context.getResultSender().lastResult(key);
	  } else {
	  context.getResultSender().sendResult(key);
	   pr.getCache().getDistributedSystem().disconnect();
      }      
    }
    else if (context.getArguments() instanceof Vector) {
      Cache c = null;
      try {
        c = CacheFactory.getAnyInstance();
      }
      catch (CacheClosedException ex) {
        vals.add("NoCacheResult");
        context.getResultSender().lastResult(vals);
      }
      Region region = c.getRegion("partition_region");
      Vector keys = (Vector)context.getArguments();
      System.out.println("Context.getArguments " + keys);
      Iterator itr = keys.iterator();
      while (itr.hasNext()) {
        Object k = itr.next();
        vals.add(region.get(k));
        System.out.println("vals " + vals);
      }
    }

	if(rs.isLocallyExecuted()){	 
		System.out.println("rs.isLocallyExecuted() ");
	   context.getResultSender().lastResult(vals);
	  } else { 	   
	  System.out.println("not rs.isLocallyExecuted() ");
	   context.getResultSender().sendResult(vals);       
	   pr.getCache().getDistributedSystem().disconnect();
      } 
  }

  private void stopServer() {
    CacheServer server = (CacheServer)GemFireCacheImpl.getInstance()
        .getCacheServers().get(0);
    server.stop();
  }
  
  private void throwException() {
    throw new InternalFunctionInvocationTargetException(
        new NullPointerException(
            "I have been thrown from RegionOperationsHAFunctionPrSHOP"));
  }

  public String getId() {
    return "RegionOperationsHAFunctionPrSHOP";
  }

  public boolean optimizeForWrite() {
    return false;
  }

  public void init(Properties arg0) {

  }

}
