/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package javaobject;


import java.util.*;
import java.io.*;
import com.gemstone.gemfire.*; // for DataSerializable
import com.gemstone.gemfire.cache.Declarable;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;

public class MultiPutFunctionI extends FunctionAdapter implements Declarable{

  public void execute(FunctionContext context) {
    ArrayList vals = new ArrayList();
    if (context.getArguments() instanceof Boolean) {
    }
    else if (context.getArguments() instanceof String) {
        String key = (String)context.getArguments();
    }
    else if(context.getArguments() instanceof Vector ) {
       Cache c = null;
       try {
           c = CacheFactory.getAnyInstance();
       } catch (CacheClosedException ex)
       {
           System.out.println("in MutiGetFunctionI.execute: no cache found ");
           vals.add("NoCacheResult");
       }

       Region region = c.getRegion("partition_region");
       Vector keys = (Vector)context.getArguments();
       System.out.println("Context.getArguments " + keys);
       Iterator itr = keys.iterator();
       while (itr.hasNext()) {
         Object k = itr.next();
         vals.add(region.get(k));
         region.put(k, k);
         System.out.println("vals " + vals);
       }
    }
  }

  public String getId() {
    return "MultiPutFunctionI";
  }

  public void init(Properties arg0) {

  }
  
  public boolean hasResult() {
	return false;
  }
  
  public boolean isHA() {
    return false;
  }
}
