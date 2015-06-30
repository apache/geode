/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.rest.internal.web.controllers;

import java.util.ArrayList;
import java.util.Collection;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.query.FunctionDomainException;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.TypeMismatchException;


/**
* The GetDeliveredOrders class is a gemfire function that gives details about delivered orders.
* <p/>
* @author Nilkanth Patel
* @since 8.0
*/

public class GetDeliveredOrders  implements Function {
  public void execute(FunctionContext context) {
    
    Cache c = null;
    ArrayList<Object> vals = new ArrayList<Object>();
    try {
      c = CacheFactory.getAnyInstance();
    } catch (CacheClosedException ex) {
      vals.add("NoCacheFoundResult");
      context.getResultSender().lastResult(vals);
    }
    
    String oql = "SELECT o.purchaseOrderNo, o.deliveryDate  FROM /orders o WHERE o.deliveryDate != NULL";
    final Query query = c.getQueryService().newQuery(oql);
   
    SelectResults result = null;
    try {
      result = (SelectResults) query.execute();
      int resultSize = result.size();
      
      if(result instanceof Collection<?>)
        for(Object item : result){
          vals.add(item);
        }
    } catch (FunctionDomainException e) {
      if(c != null)
        c.getLogger().info("Caught FunctionDomainException while executing function GetDeliveredOrders: " + e.getMessage());
        
    } catch (TypeMismatchException e) {
      if(c != null)
        c.getLogger().info("Caught TypeMismatchException while executing function GetDeliveredOrders: " + e.getMessage());
      
    } catch (NameResolutionException e) {
      if(c != null)
        c.getLogger().info("Caught NameResolutionException while executing function GetDeliveredOrders: " + e.getMessage());
      
    } catch (QueryInvocationTargetException e) {
      if(c != null)
        c.getLogger().info("Caught QueryInvocationTargetException while executing function GetDeliveredOrders: " + e.getMessage());
      
    }
    
    context.getResultSender().lastResult(vals);
  }

  public String getId() {
    return "GetDeliveredOrders";
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
