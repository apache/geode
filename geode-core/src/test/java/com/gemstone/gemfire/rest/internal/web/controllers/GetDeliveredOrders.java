/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
