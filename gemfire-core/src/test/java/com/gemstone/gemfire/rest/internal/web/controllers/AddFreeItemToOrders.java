/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.rest.internal.web.controllers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Vector;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.query.FunctionDomainException;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.TypeMismatchException;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.pdx.PdxInstance;

/**
 * Gemfire function to add free items in the existing order
 * if the total price for that order is greater then the argument
 * @author Nilkanth Patel
 */

public class AddFreeItemToOrders implements Function  {
  
  public void execute(FunctionContext context) {

    Cache c = null;
    Region r = null;
    List<Object> vals = new ArrayList<Object>();
    List<Object> keys = new ArrayList<Object>();
    List<Object> argsList = new ArrayList<Object>();
    Object[] argsArray = null;
    
    if (context.getArguments() instanceof Boolean) {
    
    } else if (context.getArguments() instanceof String) {
      String arg = (String) context.getArguments();
    }else if(context.getArguments() instanceof Vector ) {
      
    }else if (context.getArguments() instanceof Object[]) {
      argsArray = (Object[]) context.getArguments();
      argsList = Arrays.asList(argsArray);
    }else {
      System.out.println("AddFreeItemToOrders : Invalid Arguments");
    }
    
    try {
      c = CacheFactory.getAnyInstance();
      ((GemFireCacheImpl)c).getCacheConfig().setPdxReadSerialized(true);
      r = c.getRegion("orders");
    } catch (CacheClosedException ex) {
      vals.add("NoCacheFoundResult");
      context.getResultSender().lastResult(vals);
    }

    String oql = "SELECT DISTINCT entry.key FROM /orders.entries entry WHERE entry.value.totalPrice > $1";
    Object queryArgs[] = new Object[1];
    queryArgs[0] = argsList.get(0);
    
    final Query query = c.getQueryService().newQuery(oql);

    SelectResults result = null;
    try {
      result = (SelectResults) query.execute(queryArgs);
      int resultSize = result.size();
      
      if (result instanceof Collection<?>)  
        for (Object item : result) {
          keys.add(item);
        }
    } catch (FunctionDomainException e) {
     if(c != null)
      c.getLogger().info("Caught FunctionDomainException while executing function AddFreeItemToOrders: " + e.getMessage());
      
    } catch (TypeMismatchException e) {
      if(c != null)
        c.getLogger().info("Caught TypeMismatchException while executing function AddFreeItemToOrders: " + e.getMessage()); 
    } catch (NameResolutionException e) {
      if(c != null)
        c.getLogger().info("Caught NameResolutionException while executing function AddFreeItemToOrders: "  + e.getMessage());
    } catch (QueryInvocationTargetException e) {
      if(c != null)
        c.getLogger().info("Caught QueryInvocationTargetException while executing function AddFreeItemToOrders" + e.getMessage());
    }
    
    //class has to be in classpath.
    try {
      Item it = (Item)(argsList.get(1));
      for(Object key : keys)
      {
        Object obj = r.get(key);
        if(obj instanceof PdxInstance) {
          PdxInstance pi = (PdxInstance)obj;
          Order receivedOrder = (Order)pi.getObject();
          receivedOrder.addItem(it);
              
          r.put(key, receivedOrder);
        }
      }
      
      context.getResultSender().lastResult("success");
      
    }catch (ClassCastException e) {
      
      context.getResultSender().lastResult("failure");
    }catch (Exception e) {
      context.getResultSender().lastResult("failure");
    }
    
  }

  public String getId() {
    return "AddFreeItemToOrders";
  }

  @Override
  public boolean hasResult() {
    return true;
  }

  @Override
  public boolean optimizeForWrite() {
    return false;
  }

  @Override
  public boolean isHA() {
    return false;
  }
}
