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
import org.apache.geode.*; // for DataSerializable
import org.apache.geode.cache.Declarable;

import org.apache.geode.cache.*;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;

public class MultiGetFunctionI extends FunctionAdapter implements Declarable{

  public void execute(FunctionContext context) {
    ArrayList vals = new ArrayList();
    if (context.getArguments() instanceof Boolean) {
	context.getResultSender().lastResult((Boolean)context.getArguments());
    }
    else if (context.getArguments() instanceof String) {
        String key = (String)context.getArguments();
	context.getResultSender().lastResult(key);
    }
    else if(context.getArguments() instanceof Vector ) {
       Cache c = null;
       try {
           c = CacheFactory.getAnyInstance();
       } catch (CacheClosedException ex)
       {
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
    context.getResultSender().lastResult(vals);
  }

  public String getId() {
    return "MultiGetFunctionI";
  }

  public void init(Properties arg0) {

  }

}
